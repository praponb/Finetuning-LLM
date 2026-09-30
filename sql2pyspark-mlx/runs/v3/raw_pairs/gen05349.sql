SELECT CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END rating_bucket, AVG(rating) avg_rating
FROM suppliers
WHERE supplier_name LIKE '%Tok%' OR country = 'UK'
GROUP BY rating_bucket
HAVING avg_rating > 2.3;
