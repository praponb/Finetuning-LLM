SELECT * FROM (SELECT region, city, sqft FROM stores WHERE city <> 'Berlin')
PIVOT (SUM(sqft) FOR city IN ('Singapore', 'Bangkok'))
ORDER BY region
