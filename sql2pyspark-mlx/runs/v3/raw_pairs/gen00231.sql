SELECT city, SUM(age) AS total_age, SUM(SUM(age)) OVER (ORDER BY city) AS cumulative_age FROM customers GROUP BY city ORDER BY city DESC;
