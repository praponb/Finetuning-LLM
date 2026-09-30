SELECT customer_id, browser, LOWER(device) AS device_lower
FROM sessions
WHERE customer_id = 1 AND (country NOT IN ('UK', 'SG') OR device <> 'tablet')
