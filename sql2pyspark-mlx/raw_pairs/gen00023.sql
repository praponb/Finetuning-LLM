WITH exploded AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS key, attr_value
),
flattened AS (
    SELECT
        id,
        created_at,
        attr_value,
        element
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) t AS element
),
cleaned AS (
    SELECT *, DATE_TRUNC('YEAR', created_at) AS created_at_period
    FROM flattened
    WHERE element = 'BUNDLE'
),
filtered AS (
    SELECT
        id,
        created_at,
        attr_value,
        element,
        created_at_period,
        HOUR(created_at) AS created_at_hour
    FROM cleaned
    WHERE created_at > TIMESTAMP '2025-03-02 03:00:00'
      AND element IS NOT NULL
),
scored AS (
    SELECT
        id,
        created_at,
        attr_value,
        DENSE_RANK() OVER w_running AS created_at_hour_dense_rank,
        MAX(created_at_hour) OVER (PARTITION BY element) AS element_max_created_at_hour
    FROM filtered
    WHERE element = 'BUNDLE'
    WINDOW w_running AS (PARTITION BY element ORDER BY created_at_hour DESC NULLS FIRST)
),
base AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), created_at) / 365.25 AS years_since_created_at
    FROM scored
    WHERE TO_DATE(created_at) >= '2024-04-01'
)
SELECT
    id,
    created_at,
    CUME_DIST() OVER (ORDER BY element_max_created_at_hour DESC) AS element_max_created_at_hour_cume_dist,
    DENSE_RANK() OVER (ORDER BY years_since_created_at) AS years_since_created_at_dense_rank,
    MAX(created_at_hour_dense_rank) OVER (PARTITION BY MONTH(created_at) ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS created_at_hour_dense_rank_max_1h,
    MAX(created_at_hour_dense_rank) OVER (PARTITION BY YEAR(created_at) ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS created_at_hour_dense_rank_max_15m
FROM base;
