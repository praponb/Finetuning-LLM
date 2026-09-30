SELECT o.status, CASE WHEN o.amount >= 1460 THEN 'high' WHEN o.amount >= 630 THEN 'mid' ELSE 'low' END amount_bucket, SUM(CASE WHEN p.method = 'cash' THEN 1 ELSE 0 END) cash_count
FROM payments p
RIGHT OUTER JOIN orders o ON o.id = p.order_id
GROUP BY o.status, CASE WHEN o.amount >= 1460 THEN 'high' WHEN o.amount >= 630 THEN 'mid' ELSE 'low' END
ORDER BY cash_count DESC, o.status, amount_bucket
LIMIT 5;
