SELECT
    *,
    SUM(credit_limit) OVER (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
    AVG(age) OVER (PARTITION BY city ORDER BY credit_limit, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS age_avg_6_prior
FROM customers
