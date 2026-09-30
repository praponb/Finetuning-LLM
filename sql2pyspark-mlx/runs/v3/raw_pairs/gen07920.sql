SELECT
    id,
    country,
    credit_limit,
    SUM(credit_limit) OVER (PARTITION BY country) country_sum_credit_limit
FROM customers
WHERE email LIKE '%@startup.io'
ORDER BY 4 DESC
