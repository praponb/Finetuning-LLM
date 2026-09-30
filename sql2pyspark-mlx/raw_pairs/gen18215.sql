SELECT
    id,
    device,
    customer_id,
    country,
    LOWER(browser) AS browser_lower
FROM sessions;
