SELECT
    *,
    MIN(credit_limit) OVER (PARTITION BY city) AS city_min_credit_limit,
    AVG(credit_limit) OVER (ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS credit_limit_avg_2,
    AVG(credit_limit) OVER w_part AS credit_limit_avg_2_2
FROM customers
WINDOW w_part AS (PARTITION BY country ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
