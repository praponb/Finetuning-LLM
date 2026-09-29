SELECT CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 1.5 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(CASE WHEN city = 'Berlin' THEN rating ELSE 0 END) AS berlin_rating
FROM suppliers
GROUP BY rating_bucket
HAVING SUM(CASE WHEN city = 'Berlin' THEN rating ELSE 0 END) > 42
