select id, city, sqft, max(sqft) over (partition by city) as city_max_sqft from stores where store_name like 'A%';
