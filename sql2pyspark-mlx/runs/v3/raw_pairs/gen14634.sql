SELECT *, CONCAT(country, '/', device) AS country_device
FROM sessions
WHERE customer_id IS NULL AND customer_id IN (15, 25, 26)
ORDER BY id DESC;
