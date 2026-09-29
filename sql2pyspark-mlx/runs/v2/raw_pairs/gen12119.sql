SELECT CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END AS rating_bucket, city, COUNT(DISTINCT rating) AS unique_citys FROM suppliers GROUP BY 1, 2 ORDER BY city
