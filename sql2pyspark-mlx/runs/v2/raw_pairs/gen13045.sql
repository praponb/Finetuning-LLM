SELECT
    id,
    country,
    age,
    city,
    credit_limit,
    DENSE_RANK() OVER (PARTITION BY country ORDER BY age) AS dense_rnk,
    ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY city), 3) AS credit_limit_share
FROM customers
ORDER BY age DESC;
