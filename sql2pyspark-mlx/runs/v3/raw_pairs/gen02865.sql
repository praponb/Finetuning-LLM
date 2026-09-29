SELECT *, LENGTH(country) AS country_length, SUBSTRING(device, 1, 3) AS device_prefix
FROM sessions
WHERE customer_id IN (15, 18, 21) AND browser IN ('Chrome', 'Safari') AND browser IS NOT NULL
ORDER BY started_at, id
LIMIT 5
