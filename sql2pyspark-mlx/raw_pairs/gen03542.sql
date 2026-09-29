SELECT
    id,
    customer_id,
    DATE_FORMAT(started_at, 'yyyy-MM-dd') AS started_at_label,
    CAST(started_at AS DATE) AS started_at_day
FROM sessions
WHERE customer_id IS NOT NULL OR customer_id IS NULL
ORDER BY started_at_day DESC;
