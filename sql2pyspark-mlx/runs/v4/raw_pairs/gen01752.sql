SELECT
    started_at,
    country,
    customer_id,
    UPPER(browser) browser_upper
FROM sessions
WHERE device != 'mobile' AND country NOT IN ('JP', 'DE') AND browser = 'Edge';
