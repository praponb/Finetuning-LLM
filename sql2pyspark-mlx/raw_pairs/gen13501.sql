SELECT country, CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(DISTINCT rating) AS unique_countrys
FROM suppliers
GROUP BY country, rating_bucket
