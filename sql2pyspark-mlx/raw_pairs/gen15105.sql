SELECT CASE WHEN a.status IN ('FROZEN', 'OPEN') THEN 'priority' ELSE 'other' END AS status_group, MAX(t.amount) - MIN(t.amount) AS amount_range
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON c.id = a.customer_id
GROUP BY CASE WHEN a.status IN ('FROZEN', 'OPEN') THEN 'priority' ELSE 'other' END
ORDER BY status_group;
