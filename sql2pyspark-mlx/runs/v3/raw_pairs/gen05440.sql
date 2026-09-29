SELECT
    customer_id,
    country,
    browser,
    DAY(started_at) AS started_at_day
FROM sessions
WHERE NOT (device NOT IN ('tablet', 'mobile', 'desktop') AND browser IN ('Chrome', 'Edge'));
