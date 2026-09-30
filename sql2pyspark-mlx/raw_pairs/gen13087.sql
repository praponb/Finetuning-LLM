SELECT city, CASE WHEN sqft >= 13432 THEN 'high' WHEN sqft >= 5796 THEN 'mid' ELSE 'low' END AS sqft_bucket, MAX(sqft) AS max_sqft
FROM stores
GROUP BY city, sqft_bucket
HAVING MAX(sqft) < 4500
