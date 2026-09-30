SELECT * FROM (SELECT region, city, sqft FROM stores) src PIVOT (MAX(sqft) AS total FOR city IN ('New York' AS new_york, 'Bangkok' AS bangkok))
