SELECT QUARTER(paid_at) AS paid_quarter, order_id, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM payments
WHERE order_id IN (2, 15, 28) OR paid_at < '2024-07-01'
GROUP BY 1, 2
HAVING ROUND(STDDEV(amount), 2) < 30
ORDER BY stddev_amount DESC
