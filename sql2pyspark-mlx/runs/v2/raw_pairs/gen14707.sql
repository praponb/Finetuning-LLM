SELECT country, SUM(age) AS total_age, COUNT(*) AS cnt, DENSE_RANK() OVER (ORDER BY SUM(age) DESC) AS age_rank FROM customers WHERE name IS NOT NULL GROUP BY country ORDER BY cnt DESC;
