SELECT CASE WHEN c.segment IN ('Corporate', 'SMB') THEN 'group_a' ELSE 'other' END segment_group, MAX(p.paid_at) latest_paid_at
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY CASE WHEN c.segment IN ('Corporate', 'SMB') THEN 'group_a' ELSE 'other' END
ORDER BY 2, segment_group
LIMIT 10;
