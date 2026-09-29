SELECT CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END rating_bucket, SUM(rating) total_rating, COUNT(DISTINCT rating) unique_countrys
FROM suppliers
GROUP BY CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END
HAVING SUM(rating) >= 12;
