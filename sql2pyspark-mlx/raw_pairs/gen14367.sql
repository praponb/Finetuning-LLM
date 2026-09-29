SELECT * FROM (SELECT region, city, sqft FROM stores) src PIVOT (SUM(sqft) AS total, COUNT(1) AS n FOR city IN ('New York' AS new_york, 'Berlin' AS berlin))
