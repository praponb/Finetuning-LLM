SELECT * FROM (SELECT region, city, sqft FROM stores WHERE region IN ('East', 'West', 'North'))
PIVOT (COUNT(sqft) FOR city IN ('Tokyo', 'Singapore', 'Chiang Mai'))
