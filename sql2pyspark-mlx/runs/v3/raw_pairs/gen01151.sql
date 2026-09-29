SELECT country, SUM(age) AS total_age, DENSE_RANK() OVER (ORDER BY SUM(age) DESC) AS age_rank
FROM customers
WHERE NOT is_active
GROUP BY country;
