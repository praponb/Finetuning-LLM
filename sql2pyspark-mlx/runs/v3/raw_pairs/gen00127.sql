SELECT CASE WHEN shipping_fee >= 30 THEN 'high' WHEN shipping_fee >= 6 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket, SUM(amount) AS total_amount, MAX(amount) - MIN(amount) AS amount_range
FROM orders
WHERE NOT (shipping_fee BETWEEN 30 AND 39 AND order_date BETWEEN '2023-03-01' AND '2024-02-01')
GROUP BY CASE WHEN shipping_fee >= 30 THEN 'high' WHEN shipping_fee >= 6 THEN 'mid' ELSE 'low' END
ORDER BY 2
