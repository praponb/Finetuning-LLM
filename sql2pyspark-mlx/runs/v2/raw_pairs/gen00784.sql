SELECT country, CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(*) AS supplier_count
FROM suppliers
WHERE rating > 3.7
GROUP BY country, CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END
HAVING supplier_count > 1;
