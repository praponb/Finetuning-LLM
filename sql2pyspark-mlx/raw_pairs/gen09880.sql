SELECT CASE WHEN age >= 70 THEN 'high' WHEN age >= 35 THEN 'mid' ELSE 'low' END age_bucket, city, COUNT(*) row_count
FROM customers
GROUP BY CASE WHEN age >= 70 THEN 'high' WHEN age >= 35 THEN 'mid' ELSE 'low' END, city
HAVING row_count >= 2
ORDER BY row_count;
