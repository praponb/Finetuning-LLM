WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(country) AS country_lower,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM customers
    WHERE signup_date < '2025-06-01'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(age, 2) OVER (PARTITION BY segment ORDER BY credit_limit, id) AS next_age,
        SUM(credit_limit) OVER (PARTITION BY MONTH(signup_date), country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit,
        LEAD(credit_limit, 1) OVER (PARTITION BY city, segment ORDER BY signup_date, id) AS next_credit_limit,
        LAST_VALUE(credit_limit, TRUE) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit
    FROM cleaned
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date < s.signup_date
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY credit_limit DESC, id) AS dedup_rank
    FROM eligible
    QUALIFY dedup_rank = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    city,
    measure,
    value
FROM (
    SELECT
        id,
        city,
        last_known_credit_limit,
        credit_limit
    FROM latest
) src
UNPIVOT (
    value FOR measure IN (last_known_credit_limit, credit_limit AS CREDIT_LIMIT_VAL)
) unp;
