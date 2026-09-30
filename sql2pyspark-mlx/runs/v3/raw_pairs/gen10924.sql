SELECT
    sessions.id,
    sessions.country,
    sessions.customer_id,
    sessions.device,
    customers.city,
    customers.credit_limit,
    customers.segment,
    SUBSTRING(sessions.device, 1, 2) AS device_prefix
FROM sessions
INNER JOIN customers ON sessions.customer_id = customers.id;
