SELECT CASE WHEN discount >= 0.41 THEN 'high' WHEN discount >= 0.1 THEN 'mid' ELSE 'low' END AS discount_bucket, SUM(amount) AS total_amount, COUNT(*) AS cnt
FROM orders
GROUP BY CASE WHEN discount >= 0.41 THEN 'high' WHEN discount >= 0.1 THEN 'mid' ELSE 'low' END
ORDER BY discount_bucket DESC
