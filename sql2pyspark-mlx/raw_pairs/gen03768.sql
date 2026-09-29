WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM carts
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        c.segment,
        c.city
    FROM lines s
    LEFT JOIN customers c
        ON s.customer_id = c.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        segment,
        CONCAT_WS(',', city, element) AS city_element_key
    FROM joined
    WHERE segment = 'Consumer'
),
with_dims AS (
    -- Attach customers attributes
    SELECT s.*, p.age
    FROM cleaned s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.city_element_key,
        c.signup_date,
        c.credit_limit
    FROM with_dims a
    INNER JOIN customers c
        ON a.customer_id = c.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    CASE WHEN credit_limit >= 13100 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
    PERCENTILE_APPROX(credit_limit, 0.5) AS credit_limit_p50,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(credit_limit, ARRAY(0.25, 0.5, 0.75)) AS credit_limit_quartiles,
    MAX(credit_limit) AS max_credit_limit,
    MAX(credit_limit) AS max_credit_limit_2
FROM enriched
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', signup_date), CASE WHEN credit_limit >= 13100 THEN 'LARGE' ELSE 'SMALL' END), (DATE_TRUNC('QUARTER', signup_date)), ());
