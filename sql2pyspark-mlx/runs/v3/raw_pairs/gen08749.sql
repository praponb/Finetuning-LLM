SELECT
    id,
    country,
    customer_id,
    CONCAT(device, ', ', browser) AS device_browser
FROM sessions
WHERE customer_id IS NOT NULL;
