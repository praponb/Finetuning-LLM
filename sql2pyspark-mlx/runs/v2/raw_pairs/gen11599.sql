SELECT order_id, ROUND((100 * SUM(CASE WHEN method = 'cash' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_cash, SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
FROM payments
WHERE amount > 1520 AND HOUR(paid_at) BETWEEN 6 AND 10 AND status <> 'REFUNDED'
GROUP BY order_id
ORDER BY failed_count DESC, order_id DESC;
