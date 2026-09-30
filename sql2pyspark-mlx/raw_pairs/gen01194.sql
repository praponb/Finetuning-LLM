WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS tag
    FROM carts
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        d.signup_date
    FROM lines e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(signup_date) AS signup_date_month_end
    FROM joined
    WHERE customer_id IS NOT NULL
),
joined_carts AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.signup_date_month_end,
        p.credit_limit
    FROM valid e
    LEFT JOIN customers p
        ON e.customer_id <=> p.id
),
joined_carts_2 AS (
    -- Attach customers attributes
    SELECT
        a.*,
        c.signup_date,
        c.segment
    FROM joined_carts a
    LEFT JOIN customers c
        ON a.customer_id = c.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
    PERCENTILE_APPROX(credit_limit, 0.9) AS credit_limit_p90,
    GROUPING(DATE_TRUNC('MONTH', signup_date)) AS grouping_id
FROM joined_carts_2
GROUP BY ROLLUP (DATE_TRUNC('MONTH', signup_date))
ORDER BY activity_month NULLS FIRST;
