SELECT CASE WHEN amount >= 1640 THEN 'high' WHEN amount >= 510 THEN 'mid' ELSE 'low' END AS amount_bucket, ROUND(AVG(amount), 2) AS avg_amount, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
GROUP BY 1
HAVING stddev_amount > 27
