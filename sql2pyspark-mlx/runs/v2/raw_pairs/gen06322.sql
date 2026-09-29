SELECT s.id, s.started_at, s.browser
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE segment <> 'SMB')
