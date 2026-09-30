select id, city, sqft, min(sqft) over (partition by city) as city_min_sqft from stores order by city
