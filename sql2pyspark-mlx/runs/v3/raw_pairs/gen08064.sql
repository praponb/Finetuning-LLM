SELECT *, UPPER(device) AS device_upper, LOWER(device) AS device_lower FROM sessions WHERE customer_id IS NULL
