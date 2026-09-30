SELECT * FROM (SELECT region, city, sqft FROM stores WHERE opened_date >= '2023-10-18')
PIVOT (COUNT(sqft) FOR city IN ('Tokyo', 'Singapore', 'Bangkok', 'Chiang Mai'))
