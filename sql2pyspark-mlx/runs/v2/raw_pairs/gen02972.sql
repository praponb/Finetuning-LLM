SELECT country, customer_id, LENGTH(device) AS device_length
FROM sessions
WHERE NOT (browser IN ('Chrome', 'Edge') OR browser IN ('Firefox', 'Edge', 'Safari'))
ORDER BY device_length, customer_id DESC;
