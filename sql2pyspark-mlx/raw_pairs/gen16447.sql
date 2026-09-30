SELECT CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 1.4 THEN 'mid' ELSE 'low' END AS rating_bucket, MIN(rating) AS min_rating
FROM suppliers
WHERE city NOT IN ('Chiang Mai', 'Berlin', 'Tokyo') AND country IN ('SG', 'TH', 'DE')
GROUP BY rating_bucket;
