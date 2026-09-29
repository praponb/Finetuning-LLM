SELECT country, browser, INITCAP(TRIM(device)) AS device_title
FROM sessions
WHERE customer_id IN (12, 26, 33, 40);
