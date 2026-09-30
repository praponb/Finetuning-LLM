SELECT
    id,
    device,
    started_at,
    customer_id,
    LENGTH(browser) AS browser_length
FROM sessions
WHERE (started_at >= '2025-01-01' AND customer_id IS NULL) OR customer_id IS NOT NULL
