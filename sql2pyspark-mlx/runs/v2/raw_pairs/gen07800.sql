SELECT city, SUM(age) AS total_age, RANK() OVER (ORDER BY SUM(age) DESC) AS rnk FROM customers GROUP BY city ORDER BY rnk DESC, 1 DESC;
