SELECT id, country, signup_date, age, age - LAG(age, 1) OVER (PARTITION BY country ORDER BY signup_date, id) AS age_change FROM customers
