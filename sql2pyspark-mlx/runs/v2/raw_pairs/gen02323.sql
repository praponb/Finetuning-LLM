SELECT city, CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.6 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(rating) - MIN(rating) AS rating_range
FROM suppliers
WHERE rating >= 2.7
GROUP BY city, CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.6 THEN 'mid' ELSE 'low' END;
