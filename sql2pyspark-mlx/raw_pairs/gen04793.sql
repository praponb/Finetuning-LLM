SELECT is_active, ROUND(STDDEV(age), 2) AS stddev_age
FROM customers
GROUP BY is_active
HAVING ROUND(STDDEV(age), 2) >= 56;
