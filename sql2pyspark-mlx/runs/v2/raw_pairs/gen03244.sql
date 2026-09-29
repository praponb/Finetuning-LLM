SELECT id, age, country, age + credit_limit AS age_plus_credit_limit FROM customers WHERE signup_date < '2024-03-19' AND NOT is_active ORDER BY id DESC
