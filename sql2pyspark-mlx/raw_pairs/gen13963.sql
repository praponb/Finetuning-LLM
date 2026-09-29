SELECT
    id,
    email,
    country,
    city,
    segment,
    is_active,
    age,
    SUM(age) OVER w_running AS running_age,
    AVG(age) OVER (PARTITION BY MONTH(signup_date), city ORDER BY credit_limit, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS age_avg_5_prior,
    CUME_DIST() OVER (PARTITION BY country ORDER BY credit_limit) AS credit_limit_cume_dist,
    RANK() OVER (PARTITION BY country ORDER BY credit_limit NULLS LAST) AS credit_limit_rank,
    LAST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit
FROM customers
WHERE segment = 'SMB'
WINDOW w_running AS (PARTITION BY YEAR(signup_date), country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
