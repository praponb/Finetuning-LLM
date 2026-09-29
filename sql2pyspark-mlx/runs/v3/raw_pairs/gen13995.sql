SELECT
    city,
    segment,
    MIN(credit_limit) AS min_credit_limit,
    ROUND(STDDEV(age), 2) AS stddev_age
FROM customers
GROUP BY city, segment;
