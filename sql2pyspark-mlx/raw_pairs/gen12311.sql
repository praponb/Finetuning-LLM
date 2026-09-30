SELECT
    browser,
    started_at,
    device,
    TO_DATE(started_at) AS started_at_date,
    UPPER(device) AS device_upper
FROM sessions
WHERE HOUR(started_at) BETWEEN 7 AND 12 OR device IN ('tablet', 'desktop', 'mobile')
