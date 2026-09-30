WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS k, attr_value
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        element
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS element
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(element) AS element_lower
    FROM lines
    WHERE element IS NOT NULL
      AND created_at >= '2025-09-01'
),
base AS (
    -- Keep recent, valid rows
    SELECT *, QUARTER(created_at) AS created_at_quarter
    FROM filtered
    WHERE element IS DISTINCT FROM 'VIP'
      AND HOUR(created_at) BETWEEN 10 AND 16
),
filtered_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN created_at_quarter >= 66 THEN 'High' WHEN created_at_quarter >= 28 THEN 'Medium' ELSE 'Low' END AS created_at_quarter_band,
        COALESCE(element, 'NONE') AS element_filled
    FROM base
    WHERE element IS DISTINCT FROM 'BUNDLE'
)
-- Ranking and gap features
SELECT
    *,
    AVG(created_at_quarter) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS created_at_quarter_avg_30m,
    MAX(created_at_quarter) OVER (PARTITION BY element ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS created_at_quarter_max_30m,
    DENSE_RANK() OVER (ORDER BY created_at_quarter DESC NULLS FIRST) AS created_at_quarter_dense_rank
FROM filtered_carts
WHERE created_at >= '2023-09-01'
