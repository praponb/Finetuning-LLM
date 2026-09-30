SELECT QUARTER(paid_at) AS paid_quarter, ROUND((100 * SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cash
FROM payments
WHERE paid_at < '2024-01-01' AND order_id = 24
GROUP BY QUARTER(paid_at)
ORDER BY paid_quarter DESC;
