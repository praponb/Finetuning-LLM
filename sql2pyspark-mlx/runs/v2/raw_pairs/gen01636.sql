SELECT CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END rating_bucket, ROUND(STDDEV(rating), 2) stddev_rating
FROM suppliers
GROUP BY CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END
