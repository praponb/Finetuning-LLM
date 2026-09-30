SELECT * FROM (SELECT region, city, sqft FROM stores)
PIVOT (AVG(sqft) FOR city IN ('Tokyo', 'Bangkok', 'London'))
