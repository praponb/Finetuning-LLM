SELECT *, CONCAT_WS('/', device, country) AS device_country_key
FROM sessions
WHERE customer_id = 19 AND browser IN ('Chrome', 'Edge');
