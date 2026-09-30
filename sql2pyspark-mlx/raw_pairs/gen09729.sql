SELECT id, segment, city, signup_date, age, LEAD(age, 1) OVER (PARTITION BY segment, city ORDER BY signup_date, id) AS next_age FROM customers WHERE email LIKE '%@GMAIL.COM'
