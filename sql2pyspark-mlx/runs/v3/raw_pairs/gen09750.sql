SELECT
    id,
    customer_id,
    country,
    browser,
    DATE_TRUNC('month', started_at) AS started_at_period,
    UPPER(browser) AS browser_upper
FROM sessions
WHERE country != 'UK' OR browser = 'Firefox'
