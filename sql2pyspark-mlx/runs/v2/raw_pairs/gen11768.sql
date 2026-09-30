SELECT id, device, LENGTH(device) AS device_length
FROM sessions
ORDER BY device_length ASC;
