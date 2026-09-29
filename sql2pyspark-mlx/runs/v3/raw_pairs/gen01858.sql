SELECT city, SUM(age) AS total_age, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(age) DESC) AS age_rank FROM customers WHERE segment = 'Consumer' GROUP BY city ORDER BY age_rank DESC, 1 LIMIT 1
