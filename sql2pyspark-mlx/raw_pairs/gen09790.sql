SELECT
    id,
    customer_id,
    device,
    TRIM(browser) AS clean_browser
FROM sessions
WHERE browser IN ('Edge', 'Chrome') AND browser = 'Firefox'
