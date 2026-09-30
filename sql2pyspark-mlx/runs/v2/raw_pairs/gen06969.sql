SELECT id, city, signup_date, country, age, RANK() OVER (PARTITION BY city ORDER BY signup_date) AS rnk, RANK() OVER (PARTITION BY country ORDER BY age) AS rnk_2 FROM customers
