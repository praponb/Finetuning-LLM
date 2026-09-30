SELECT customer_id, browser, device, LOWER(country) AS country_lower FROM sessions WHERE started_at < '2025-08-16'
