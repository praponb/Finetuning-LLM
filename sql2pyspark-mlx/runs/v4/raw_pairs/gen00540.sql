SELECT id, country, is_active, LENGTH(city) AS city_length FROM customers WHERE NOT (country = 'TH' AND credit_limit <= 14000 AND signup_date BETWEEN '2024-08-01' AND '2025-07-01') ORDER BY id DESC
