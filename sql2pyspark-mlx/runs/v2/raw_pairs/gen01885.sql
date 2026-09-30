SELECT country, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit, COUNT(*) AS cnt
FROM customers
WHERE country NOT IN ('DE', 'JP') AND (country = 'US' OR city IN ('Chiang Mai', 'New York'))
GROUP BY country;
