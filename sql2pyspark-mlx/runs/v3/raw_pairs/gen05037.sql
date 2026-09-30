SELECT
    device,
    country,
    browser,
    CONCAT(device, ', ', country) device_country
FROM sessions
