SELECT
    id,
    country,
    device,
    customer_id,
    TRIM(device) AS clean_device,
    TRIM(country) AS clean_country
FROM sessions
WHERE device = 'desktop' AND device = 'desktop'
