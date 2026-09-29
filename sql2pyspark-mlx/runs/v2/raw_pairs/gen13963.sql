SELECT CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END rating_bucket, MAX(rating) max_rating
FROM suppliers
GROUP BY CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 2.5 THEN 'mid' ELSE 'low' END
HAVING max_rating < 2.8
ORDER BY 1;
