SELECT * FROM (SELECT city, region, sqft FROM stores WHERE city = 'Chiang Mai') PIVOT (MAX(sqft) FOR region IN ('West', 'South', 'East'))
