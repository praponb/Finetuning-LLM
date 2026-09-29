WITH exploded AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS k, v
),
flattened AS (
    SELECT
        id,
        created_at,
        v,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM exploded
),
filtered AS (
    SELECT
        *,
        MONTH(created_at) AS created_at_month,
        COALESCE(value, 'NONE') AS value_filled
    FROM flattened
    WHERE value IN ('WELCOME5', 'BUNDLE', 'VIP')
),
cleaned AS (
    SELECT
        *,
        COALESCE(value, 'N/A') AS value_filled_2,
        COALESCE(value, 'UNKNOWN') AS resolved_value
    FROM filtered
    WHERE created_at_month > 27
      AND created_at > DATE '2025-03-01'
),
enriched AS (
    SELECT *, AVG(created_at_month) OVER (PARTITION BY value ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS created_at_month_avg_1h
    FROM cleaned
),
metrics AS (
    SELECT
        id,
        v,
        created_at_month_avg_1h,
        RANK() OVER (PARTITION BY value ORDER BY created_at_month DESC) AS created_at_month_rank,
        RANK() OVER (PARTITION BY value ORDER BY created_at_month DESC NULLS FIRST) AS created_at_month_rank_2
    FROM enriched
    WHERE created_at_month >= 38
)
SELECT v, created_at_month_rank
FROM metrics
