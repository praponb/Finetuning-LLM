SELECT CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(rating) AS max_rating
FROM suppliers
GROUP BY 1
