select id, sqft, region, city from stores where sqft = (select min(sqft) from stores where store_name like 'Mal%');
