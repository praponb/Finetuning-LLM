SELECT signup_date, email, CONCAT(name, ', ', segment) AS name_segment
FROM customers
WHERE NOT is_active
ORDER BY email DESC;
