SELECT id, city, age, ROUND(age / SUM(age) OVER (PARTITION BY city), 3) AS age_share FROM customers ORDER BY age DESC
