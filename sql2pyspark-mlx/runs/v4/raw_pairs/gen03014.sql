WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        price,
        promo,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM lines
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(price * 1.15, 2) AS price_adjusted,
        COALESCE(value, 'NONE') AS value_filled
    FROM lines_carts
    WHERE created_at > DATE '2025-03-01'
      AND promo IS NOT NULL
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        price,
        promo,
        value,
        price_adjusted,
        value_filled,
        UPPER(promo) AS promo_upper,
        price / NULLIF(price_adjusted, 0) AS price_per_price_adjusted
    FROM base
    WHERE created_at > TIMESTAMP '2025-03-02 11:00:00'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(price_adjusted) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS price_adjusted_count_1h,
        ROUND(price / SUM(price) OVER (PARTITION BY promo), 4) AS price_share,
        ROUND(price / SUM(price) OVER (PARTITION BY value), 4) AS price_share_2
    FROM base_carts
)
-- Grouped metrics
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    CASE WHEN price_adjusted_count_1h >= 16 THEN 'HIGH' ELSE 'LOW' END AS price_adjusted_count_1h_tier,
    SORT_ARRAY(COLLECT_SET(value)) AS value_list,
    SIZE(COLLECT_SET(value)) AS num_values,
    MIN(created_at) AS first_seen
FROM metrics
GROUP BY DATE_TRUNC('HOUR', created_at), CASE WHEN price_adjusted_count_1h >= 16 THEN 'HIGH' ELSE 'LOW' END
ORDER BY hour_window ASC NULLS LAST;
