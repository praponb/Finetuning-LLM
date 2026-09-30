SELECT city, ROUND(STDDEV(age), 2) AS stddev_age, MAX(credit_limit) AS max_credit_limit
FROM customers
WHERE country = 'JP'
GROUP BY city
HAVING ROUND(STDDEV(age), 2) >= 37;
