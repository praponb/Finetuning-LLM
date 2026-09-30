SELECT id, city, signup_date, age, age - LAG(age, 1) OVER (PARTITION BY city ORDER BY signup_date, id) AS age_change FROM customers
