select id, sqft, city, store_name from stores where sqft >= (select max(sqft) from stores where sqft >= 7954)
