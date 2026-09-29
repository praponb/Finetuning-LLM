SELECT
    id,
    device,
    country,
    browser,
    CASE WHEN browser = 'Firefox' THEN 1 ELSE 0 END AS is_firefox,
    CASE WHEN browser = 'Chrome' THEN 1 ELSE 0 END AS is_chrome
FROM sessions
WHERE customer_id IS NULL;
