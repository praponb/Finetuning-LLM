WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        t.id,
        t.customer_id,
        line.product_id,
        line.qty,
        line.promo
    FROM carts t
    LATERAL VIEW EXPLODE(t.items) x AS line
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.customer_id,
        a.product_id,
        a.promo,
        b.credit_limit,
        b.email,
        b.country
    FROM lines a
    INNER JOIN customers b
        ON a.customer_id <=> b.id
),
remaining_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM with_dims
    EXCEPT ALL
    SELECT customer_id
    FROM carts
    WHERE SIZE(coupon_codes) > 2
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id
),
kept_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM verified
    EXCEPT DISTINCT
    SELECT customer_id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
verified_carts AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
with_dims_carts AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.credit_limit,
        src.country,
        b.segment,
        b.name,
        b.credit_limit AS customer_credit_limit
    FROM verified_carts src
    LEFT JOIN customers b
        ON src.customer_id <=> b.id
    WHERE b.country = 'JP'
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.credit_limit,
        e.name,
        e.customer_credit_limit,
        c.signup_date,
        c.country AS customer_country,
        c.city
    FROM with_dims_carts e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_hist AS customer_credit_limit_cume_dist,
        CUME_DIST() OVER w_hist AS customer_credit_limit_cume_dist_2,
        ROUND(customer_credit_limit / SUM(customer_credit_limit) OVER (PARTITION BY customer_id), 4) AS customer_credit_limit_share
    FROM joined
    WINDOW w_hist AS (PARTITION BY customer_id ORDER BY customer_credit_limit DESC NULLS LAST)
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN customer_credit_limit_share >= 0.63 THEN 'LARGE' ELSE 'SMALL' END AS customer_credit_limit_share_tier,
        APPROX_PERCENTILE(customer_credit_limit_share, 0.5) AS customer_credit_limit_share_p50,
        COUNT(*) FILTER (WHERE customer_country = 'JP') AS jp_rows,
        COUNT_IF(city = 'Tokyo') AS tokyo_events
    FROM scored
    GROUP BY CASE WHEN customer_credit_limit_share >= 0.63 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    customer_credit_limit_share_tier,
    customer_credit_limit_share_p50,
    jp_rows
FROM totals
ORDER BY customer_credit_limit_share_tier DESC, jp_rows
