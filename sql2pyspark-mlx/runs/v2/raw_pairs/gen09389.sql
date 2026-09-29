SELECT ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit
FROM customers
WHERE signup_date BETWEEN '2024-03-01' AND '2024-07-09';
