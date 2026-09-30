SELECT country FROM customers UNION SELECT country FROM sessions WHERE started_at < '2024-08-19' ORDER BY country;
