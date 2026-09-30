SELECT
    customer_id,
    country,
    UPPER(browser) AS browser_upper,
    CONCAT(device, '/', country) AS device_country
FROM sessions
WHERE TO_DATE(started_at) < '2024-08-01' OR customer_id IN (29, 40);
