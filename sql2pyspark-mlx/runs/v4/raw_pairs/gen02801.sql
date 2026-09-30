SELECT
    id,
    country,
    customer_id,
    started_at
FROM sessions
WHERE country IN ('JP', 'UK') AND device != 'desktop' AND TO_DATE(started_at) < '2024-10-01';
