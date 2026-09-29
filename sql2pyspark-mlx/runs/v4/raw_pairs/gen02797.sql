SELECT o.status, ROUND((100 * SUM(CASE WHEN p.status = 'REFUNDED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_refunded, ROUND(AVG(p.amount), 2) AS avg_amount
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
GROUP BY o.status;
