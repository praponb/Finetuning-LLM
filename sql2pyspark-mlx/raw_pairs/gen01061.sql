SELECT
    id,
    started_at,
    country,
    customer_id
FROM sessions
WHERE customer_id IS NOT NULL
