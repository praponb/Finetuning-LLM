SELECT
    sessions.id,
    sessions.started_at,
    sessions.browser,
    sessions.device
FROM sessions
WHERE sessions.customer_id IN (SELECT id FROM customers WHERE credit_limit >= 13700)
