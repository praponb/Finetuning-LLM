SELECT country, MAX(age) - MIN(age) AS age_range, ROUND(STDDEV(age), 2) AS stddev_age
FROM customers
GROUP BY country
HAVING age_range > 54;
