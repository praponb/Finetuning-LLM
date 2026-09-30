SELECT id, customer_id, started_at
FROM sessions
WHERE browser NOT IN ('Firefox', 'Safari')
ORDER BY customer_id DESC, started_at DESC;
