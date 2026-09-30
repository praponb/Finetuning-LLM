SELECT ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit, MAX(credit_limit) AS max_credit_limit, MAX(age) AS max_age
FROM customers
WHERE city IN ('Chiang Mai', 'Bangkok')
