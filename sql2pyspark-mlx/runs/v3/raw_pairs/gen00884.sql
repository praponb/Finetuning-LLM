SELECT CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(CASE WHEN country = 'SG' THEN rating ELSE 0 END) AS sg_rating, MAX(rating) AS max_rating
FROM suppliers
WHERE city = 'Bangkok' AND rating BETWEEN 1.8 AND 3.2
GROUP BY CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END
HAVING MAX(rating) >= 3.4;
