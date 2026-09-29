SELECT c.segment, c.is_active, MAX(a.opened_date) AS latest_opened_date
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.id
GROUP BY c.segment, c.is_active
ORDER BY segment DESC;
