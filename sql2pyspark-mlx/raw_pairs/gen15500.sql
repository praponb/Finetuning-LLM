SELECT * FROM (SELECT CASE WHEN sqft >= 15168 THEN 'high' WHEN sqft >= 6419 THEN 'mid' ELSE 'low' END AS sqft_bucket, city, sqft FROM stores WHERE city NOT IN ('London', 'Singapore'))
PIVOT (COUNT(sqft) FOR city IN ('Singapore', 'London', 'Bangkok', 'New York'))
ORDER BY sqft_bucket;
