SELECT CASE WHEN amount >= 1210 THEN 'high' WHEN amount >= 710 THEN 'mid' ELSE 'low' END AS amount_bucket, AVG(amount) AS avg_amount
FROM payments
WHERE order_id IN (10, 20, 30)
GROUP BY amount_bucket
HAVING avg_amount > 790;
