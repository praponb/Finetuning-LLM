SELECT *, SUBSTRING(country, 1, 2) country_prefix FROM sessions WHERE customer_id = 10 AND (browser = 'Safari' OR HOUR(started_at) BETWEEN 9 AND 14);
