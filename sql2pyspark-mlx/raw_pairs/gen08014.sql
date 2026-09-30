WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS key, v
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        customer_id,
        created_at,
        key,
        element
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) t AS element
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('MONTH', created_at) created_at_period,
        DATE_TRUNC('week', created_at) created_at_period_2
    FROM flattened
    WHERE created_at > DATE '2025-03-01'
      AND element IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.credit_limit,
        d.name,
        d.segment
    FROM cleaned e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        element,
        credit_limit,
        name,
        segment,
        MAX(credit_limit) OVER (PARTITION BY customer_id, segment ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) credit_limit_max_2h
    FROM joined
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(credit_limit, 2) credit_limit_rounded,
        COALESCE(segment, 'UNKNOWN') segment_clean
    FROM scored
    WHERE segment IS NOT NULL
),
filtered_carts AS (
    -- Basic cleansing and defaults
    SELECT
        element,
        name,
        segment,
        credit_limit_max_2h,
        credit_limit_rounded,
        segment_clean,
        UPPER(segment) segment_upper
    FROM filtered
    WHERE element IN ('VIP', 'WELCOME5', 'BUNDLE')
)
-- Multi-level aggregation
SELECT
    CASE WHEN credit_limit_rounded >= 62 THEN 'HIGH' ELSE 'LOW' END AS credit_limit_rounded_tier,
    element,
    ROUND(AVG(credit_limit_rounded), 2) AS avg_credit_limit_rounded,
    COUNT(*) AS row_count,
    COUNT(*) FILTER (WHERE segment = 'Consumer') AS consumer_rows
FROM filtered_carts
GROUP BY ROLLUP (CASE WHEN credit_limit_rounded >= 62 THEN 'HIGH' ELSE 'LOW' END, element)
