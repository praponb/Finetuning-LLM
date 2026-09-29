SELECT CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 1.4 THEN 'mid' ELSE 'low' END rating_bucket, country, COUNT(*) cnt
FROM suppliers
WHERE country = 'UK' AND supplier_name LIKE '%Wholesal%'
GROUP BY CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 1.4 THEN 'mid' ELSE 'low' END, country
ORDER BY cnt, rating_bucket, country
LIMIT 5;
