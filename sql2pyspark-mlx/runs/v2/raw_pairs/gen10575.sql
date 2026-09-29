SELECT * FROM (SELECT city, region, sqft FROM stores) PIVOT (MAX(sqft) FOR region IN ('West', 'North'));
