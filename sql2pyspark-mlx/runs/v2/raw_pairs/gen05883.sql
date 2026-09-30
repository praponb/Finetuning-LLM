SELECT status, MAX(amount) - MIN(amount) AS amount_range FROM orders GROUP BY status ORDER BY status
