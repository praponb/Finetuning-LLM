SELECT id, country, age, SUM(age) OVER (PARTITION BY country) AS country_sum_age FROM customers ORDER BY country_sum_age
