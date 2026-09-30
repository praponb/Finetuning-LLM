SELECT
    sessions.id,
    sessions.browser,
    sessions.customer_id,
    sessions.started_at
FROM sessions
WHERE sessions.customer_id IN (SELECT id FROM customers WHERE signup_date <= '2023-11-01')
