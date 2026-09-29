SELECT s.device, c.city, c.is_active, SUBSTRING(c.city, 1, 4) AS city_prefix FROM sessions s JOIN customers c ON c.id = s.customer_id WHERE c.is_active = FALSE ORDER BY device ASC, city_prefix ASC
