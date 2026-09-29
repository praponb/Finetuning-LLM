SELECT
    id,
    device,
    customer_id,
    browser,
    DATE_FORMAT(started_at, 'yyyy-MM-dd') AS started_at_label
FROM sessions
WHERE browser = 'Firefox'
