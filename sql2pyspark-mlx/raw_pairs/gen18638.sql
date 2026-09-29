SELECT * FROM (SELECT CASE WHEN sqft >= 15674 THEN 'high' WHEN sqft >= 5769 THEN 'mid' ELSE 'low' END AS sqft_bucket, city, sqft FROM stores)
PIVOT (COUNT(sqft) FOR city IN ('Berlin', 'New York'))
