SELECT id, country, LENGTH(device) AS device_length
FROM sessions
WHERE device IN ('mobile', 'tablet');
