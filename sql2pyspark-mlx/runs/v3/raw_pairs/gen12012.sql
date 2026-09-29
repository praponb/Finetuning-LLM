SELECT id, signup_date
FROM customers
WHERE NOT is_active
ORDER BY signup_date DESC;
