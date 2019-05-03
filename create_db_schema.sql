CREATE DATABASE IF NOT EXISTS citybike_db;

use citybike_db;

drop table IF EXISTS d_trips;

CREATE TABLE d_trips (
  tripduration bigint,
  starttime datetime,
  stoptime datetime,
  start_station_id int,
  start_station_name varchar(100) ,
  start_station_latitude double,
  start_station_longitude double,
  end_station_id int,
  end_station_name varchar(100),
  end_station_latitude double ,
  end_station_longitude double,
  bikeid bigint,
  usertype varchar(50),
  birth_year varchar(50),
  gender int
);


-- CREATE TABLE d_trips (
--   tripduration int NOT NULL,
--   starttime datetime NOT NULL,
--   stoptime datetime NOT NULL,
--   start_station_id int NOT NULL,
--   start_station_name varchar(100) NOT NULL,
--   start_station_latitude float NOT NULL,
--   start_station_longitude float NOT NULL,
--   end_station_id int NOT NULL,
--   end_station_name varchar(100) NOT NULL,
--   end_station_latitude float NOT NULL,
--   end_station_longitude float NOT NULL,
--   bikeid int NOT NULL,
--   usertype varchar(50) NOT NULL,
--   birth_year int NOT NULL,
--   gender int NOT NULL
-- );


