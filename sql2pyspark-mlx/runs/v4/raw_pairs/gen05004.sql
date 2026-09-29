SELECT DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, COUNT(*) AS cnt, ROUND((100 * SUM(CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_refunded
FROM payments
WHERE amount >= 1060
GROUP BY DATE_FORMAT(paid_at, 'yyyy-MM');
