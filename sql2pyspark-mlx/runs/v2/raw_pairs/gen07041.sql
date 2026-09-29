SELECT o.channel, ROUND((100 * SUM(CASE WHEN p.method = 'bank_transfer' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_bank_transfer, MAX(p.paid_at) latest_paid_at
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
GROUP BY o.channel
ORDER BY latest_paid_at;
