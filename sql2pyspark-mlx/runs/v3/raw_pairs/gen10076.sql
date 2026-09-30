SELECT id, city, signup_date, age, LEAD(age, 2) OVER (PARTITION BY city ORDER BY signup_date, id) AS next_age FROM customers ORDER BY signup_date
