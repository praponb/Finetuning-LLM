WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        IF(is_active, 'Yes', 'No') is_active_label,
        COALESCE(city, 'NONE') resolved_city
    FROM customers
    WHERE email NOT LIKE '%@company.co'
      AND city IS DISTINCT FROM 'New York'
)
-- Running and moving aggregates
SELECT
    id,
    name,
    email,
    city,
    segment,
    signup_date,
    credit_limit,
    is_active_label,
    ROUND(age / SUM(age) OVER (PARTITION BY city, country), 4) age_share,
    LAST_VALUE(age) IGNORE NULLS OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_age,
    LAST_VALUE(city, TRUE) OVER (PARTITION BY segment, city ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_city,
    AVG(credit_limit) OVER (PARTITION BY YEAR(signup_date), country ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) credit_limit_avg_2_prior,
    CUME_DIST() OVER (ORDER BY age DESC NULLS LAST) age_cume_dist
FROM base
