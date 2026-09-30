SELECT o.status, ROUND((100 * SUM(CASE WHEN p.method = 'bank_transfer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_bank_transfer
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
GROUP BY o.status
ORDER BY o.status, pct_bank_transfer DESC
