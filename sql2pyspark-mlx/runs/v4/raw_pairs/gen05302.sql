WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS value
    FROM carts
),
joined AS (
    -- Attach customers attributes
    SELECT s.*, c.signup_date
    FROM exploded s
    INNER JOIN customers c
        ON s.customer_id = c.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.signup_date AS customer_signup_date,
        b.credit_limit
    FROM joined s
    LEFT JOIN customers b
        ON s.customer_id = b.id
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', signup_date) AS week_start,
    CASE WHEN credit_limit >= 12400 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    COUNT_IF(value = 'WELCOME5') AS welcome5_events,
    MIN(credit_limit) AS min_credit_limit,
    SIZE(COLLECT_SET(value)) AS num_values,
    GROUPING_ID() AS grouping_level
FROM enriched
GROUP BY CUBE (DATE_TRUNC('WEEK', signup_date), CASE WHEN credit_limit >= 12400 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', signup_date));
