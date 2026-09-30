SELECT id, age, signup_date, email, ROUND(age * 1.15, 2) age_scaled, LENGTH(name) name_length FROM customers WHERE name NOT LIKE '%Alic%' AND name IS NOT NULL AND MONTH(signup_date) IN (2, 5, 9)
