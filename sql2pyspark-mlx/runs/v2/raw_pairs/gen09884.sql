SELECT id, city, name, FLOOR(age) AS age_whole FROM customers WHERE segment NOT IN ('Corporate', 'Consumer', 'SMB') AND signup_date BETWEEN '2025-01-03' AND '2025-04-01'
