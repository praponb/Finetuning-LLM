SELECT CASE WHEN shipping_fee >= 40 THEN 'high' WHEN shipping_fee >= 10 THEN 'mid' ELSE 'low' END AS shipping_fee_bucket, MAX(amount) AS max_amount
FROM orders
WHERE YEAR(order_date) >= 2025 AND channel IS NOT NULL
GROUP BY CASE WHEN shipping_fee >= 40 THEN 'high' WHEN shipping_fee >= 10 THEN 'mid' ELSE 'low' END
HAVING MAX(amount) > 990;
