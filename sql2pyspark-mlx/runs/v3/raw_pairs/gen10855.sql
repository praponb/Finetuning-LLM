SELECT * FROM (SELECT city, region, sqft FROM stores WHERE LOWER(store_name) LIKE '%iam%')
PIVOT (MAX(sqft) FOR region IN ('West', 'North', 'East', 'South'))
ORDER BY city;
