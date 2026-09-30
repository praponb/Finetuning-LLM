WITH base AS (
    SELECT
        *,
        LOWER(country) AS country_lower,
        COALESCE(segment, 'NONE') AS segment_clean
    FROM customers
    WHERE age != 37
),
with_windows AS (
    SELECT
        *,
        LAST_VALUE(segment) IGNORE NULLS OVER (PARTITION BY country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_segment,
        DENSE_RANK() OVER w_recent AS credit_limit_dense_rank
    FROM base
    WHERE country IN ('US', 'JP')
    WINDOW w_recent AS (PARTITION BY YEAR(signup_date), city ORDER BY credit_limit DESC NULLS FIRST)
),
metrics AS (
    SELECT
        id,
        name,
        segment,
        is_active,
        segment_clean,
        last_known_segment,
        FIRST_VALUE(city) IGNORE NULLS OVER (PARTITION BY country ORDER BY credit_limit_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        SUM(credit_limit_dense_rank) OVER (PARTITION BY segment, city) AS segment_sum_credit_limit_dense_rank
    FROM with_windows
),
not_flagged AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.country IN ('DE', 'US', 'TH')
        )
)
SELECT
    id,
    segment,
    is_active,
    segment_clean,
    last_known_segment,
    segment_sum_credit_limit_dense_rank
FROM not_flagged
ORDER BY segment;
