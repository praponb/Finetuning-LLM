SELECT * FROM (SELECT CASE WHEN discount >= 0.42 THEN 'high' WHEN discount >= 0.19 THEN 'mid' ELSE 'low' END AS discount_bucket, status, amount FROM orders WHERE order_date < '2025-03-01')
PIVOT (SUM(amount) FOR status IN ('CANCELLED', 'SHIPPED', 'COMPLETED'))
ORDER BY discount_bucket
