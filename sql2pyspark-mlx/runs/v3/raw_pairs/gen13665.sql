SELECT *, CONCAT(device, ' ', country) AS device_country
FROM sessions
WHERE browser = 'Chrome' AND browser IN ('Edge', 'Firefox')
