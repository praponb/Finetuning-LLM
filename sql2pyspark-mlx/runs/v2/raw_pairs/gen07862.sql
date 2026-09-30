SELECT
    device,
    customer_id,
    LENGTH(browser) AS browser_length,
    DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at
FROM sessions
WHERE device IN ('mobile', 'tablet');
