SELECT * FROM (SELECT region, city, sqft FROM stores) PIVOT (MAX(sqft) FOR city IN ('Chiang Mai', 'London', 'Bangkok', 'New York'));
