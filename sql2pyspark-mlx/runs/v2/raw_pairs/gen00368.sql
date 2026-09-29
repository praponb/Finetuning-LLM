select *, length(city) city_length, trim(city) clean_city from stores where sqft between 4534 and 5118 order by opened_date desc, id limit 3
