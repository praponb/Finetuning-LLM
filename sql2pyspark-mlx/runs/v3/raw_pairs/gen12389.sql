SELECT
    browser,
    customer_id,
    device,
    DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at,
    DATE_FORMAT(started_at, 'MMM yyyy') AS started_at_label
FROM sessions
WHERE HOUR(started_at) BETWEEN 8 AND 14;
