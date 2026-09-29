SELECT * FROM (SELECT city, region, sqft FROM stores) src PIVOT (AVG(sqft) AS total, COUNT(1) AS cnt FOR region IN ('North', 'West'))
