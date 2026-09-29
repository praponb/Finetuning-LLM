SELECT
    id,
    customer_id,
    started_at,
    browser
FROM sessions
WHERE HOUR(started_at) BETWEEN 7 AND 15 AND customer_id = 30 AND TO_DATE(started_at) >= '2025-01-01';
