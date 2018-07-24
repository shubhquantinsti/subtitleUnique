var fs = require('fs');
var jsonexport = require('jsonexport');
const mongoose = require('mongoose');
const Schema = mongoose.Schema;
mongoose.Promise = global.Promise;

mongoose.connect('mongodb://localhost/first');

mongoose.connection.once('open',function(){
  console.log('Connection has been made...');
}).on('error',function(error){
    console.log('Connection error',error);
});

var workSchema = new Schema({
                config : {
                  isFullscreen : Boolean,
                  playbackRate : Number,
                  isMuted : Boolean,
                  isPlaying : Boolean,
                  subtitle : String
                },
                userId : String,
                sectionId : String,
                unitId : String,
                courseId : String,
                eventType : String,
                currentTime : Number,
                totalDurartion : Number,
                percentage : Number,
                flag : Number,
                createdAt : Date,
                updatedAt : Date
            });

var collectionW = mongoose.model('quantravid',workSchema, 'quantravid');

var json1;
var json2;

collectionW.aggregate(
                            [
                                {$match : { 'config.isPlaying' : true , 'config.subtitle' : {$nin : ['subtitlesOff']} }},
                                {$group : { _id : {'courseId':'$courseId','sectionId':'$sectionId','unitId':'$unitId'},uniqueUserID: {$addToSet:'$userId'}}},
                                {$project : {"count_on": {$size: "$uniqueUserID"}}},
                                {$sort : {_id : 1}}
                            ],function(err,data){
                                json1 = data;

                                collectionW.aggregate(
                                            [
                                                {$match : { 'config.isPlaying' : true, 'config.subtitle' : 'subtitlesOff' }},
                                                {$group : { _id : {'courseId':'$courseId','sectionId':'$sectionId','unitId':'$unitId'},uniqueUserID: {$addToSet:'$userId'}}},
                                                {$project : {"count_off": {$size: "$uniqueUserID"}}},
                                                {$sort : {_id : 1}}
                                            ],function(err,data){
                                                json2 = data;
                                                var res = [];

                                                for(var i = 0; i < json1.length; i++){
                                                  for(var j = 0; j < json2.length; j++){
                                                    if(JSON.stringify(json1[i]._id) == JSON.stringify(json2[j]._id)){
                                                      res.push({_id: json1[i]._id, 'count_on' : json1[i].count_on, 'count_off' : json2[j].count_off});
                                                      break;
                                                    }
                                                  }
                                                }

                                                var ff = JSON.stringify(res);
                                                //console.log(ff);
                                                fs.writeFile('jsonFile.json',ff,function(){console.log('Wrote');
                                                 var reader = fs.createReadStream('jsonFile.json');
                                                 var writer = fs.createWriteStream('dataQuantraQuiz.csv');

                                                reader.pipe(jsonexport()).pipe(writer);

                                              });

                                            });

                            });
