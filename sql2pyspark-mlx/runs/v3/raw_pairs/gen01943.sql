SELECT id, signup_date, is_active, city, GREATEST(credit_limit, age) AS greatest_credit_limit_age FROM customers WHERE name LIKE 'No%' ORDER BY signup_date ASC, id ASC LIMIT 20
