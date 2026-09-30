SELECT id, segment, signup_date, age, LEAD(age, 1, 0) OVER (PARTITION BY segment ORDER BY signup_date, id) next_age FROM customers
