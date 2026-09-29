select id, opened_date, concat(region, '/', city) as region_city, lower(city) as city_lower from stores where region in ('East', 'West', 'North')
