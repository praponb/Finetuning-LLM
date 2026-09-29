WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS key, value
),
filtered AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        value,
        DATEDIFF(CURRENT_DATE(), created_at) / 30 AS months_since_created_at,
        COALESCE(key, 'OTHER') AS key_filled
    FROM flattened
    WHERE key IS DISTINCT FROM 'utm_source'
),
remaining_keys AS (
    SELECT id
    FROM filtered
    EXCEPT
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
kept_rows AS (
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.id = k.id
),
with_windows AS (
    SELECT
        value,
        months_since_created_at,
        key_filled,
        RANK() OVER (ORDER BY months_since_created_at NULLS FIRST) AS months_since_created_at_rank,
        DENSE_RANK() OVER (ORDER BY months_since_created_at NULLS LAST) AS months_since_created_at_dense_rank,
        MAX(months_since_created_at) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS months_since_created_at_max_30m,
        COUNT(months_since_created_at) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS months_since_created_at_count_15m
    FROM kept_rows
),
filtered_carts AS (
    SELECT
        value,
        months_since_created_at,
        key_filled,
        months_since_created_at_rank,
        months_since_created_at_dense_rank,
        ROUND(months_since_created_at_count_15m * 0.85, 2) AS months_since_created_at_count_15m_adjusted,
        ROUND(months_since_created_at * 1.15, 2) AS months_since_created_at_adjusted
    FROM with_windows
    WHERE months_since_created_at_count_15m >= 13
),
base AS (
    SELECT
        *,
        months_since_created_at + months_since_created_at_rank AS months_since_created_at_plus_months_since_created_at_rank,
        months_since_created_at_dense_rank - months_since_created_at_count_15m_adjusted AS months_since_created_at_dense_rank_minus_months_since_created_at_count_15m_adjusted
    FROM filtered_carts
    WHERE months_since_created_at_adjusted BETWEEN 35 AND 74
),
valid AS (
    SELECT *, months_since_created_at_rank - months_since_created_at AS months_since_created_at_rank_minus_months_since_created_at
    FROM base
    WHERE months_since_created_at_adjusted BETWEEN 74 AND 78
)
SELECT
    value,
    months_since_created_at,
    months_since_created_at_rank,
    months_since_created_at_dense_rank,
    months_since_created_at_adjusted,
    months_since_created_at_plus_months_since_created_at_rank,
    months_since_created_at_rank_minus_months_since_created_at
FROM valid;
