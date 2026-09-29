WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(age, 1, 0) OVER (ORDER BY signup_date, id) AS prev_age,
        SUM(credit_limit) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
        FIRST_VALUE(city, TRUE) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        FIRST_VALUE(city) IGNORE NULLS OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city_2,
        NTILE(10) OVER (PARTITION BY segment ORDER BY age, id) AS age_bucket
    FROM customers
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date < s.signup_date
        )
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY prev_age DESC, id) AS seq_no,
        MIN(credit_limit) OVER w_recent AS country_min_credit_limit,
        ROW_NUMBER() OVER w_recent_2 AS seq_no_2,
        STDDEV_SAMP(age) OVER (PARTITION BY city, country) AS city_stddev_samp_age,
        MAX(prev_age) OVER (PARTITION BY city) AS city_max_prev_age
    FROM not_flagged
    WINDOW w_recent AS (PARTITION BY country), w_recent_2 AS (PARTITION BY segment, country ORDER BY prev_age DESC, id)
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
        )
)
SELECT
    id,
    name,
    email,
    country,
    city,
    segment,
    is_active,
    credit_limit,
    age
FROM eligible
ORDER BY segment DESC, id
LIMIT 10
