SELECT CASE WHEN rating >= 4.5 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END rating_bucket, COUNT(DISTINCT rating) unique_countrys, SUM(CASE WHEN city = 'Bangkok' THEN rating ELSE 0 END) bangkok_rating
FROM suppliers
GROUP BY 1
HAVING unique_countrys > 1
