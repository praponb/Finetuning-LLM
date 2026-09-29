WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM (
        SELECT
            id,
            created_at,
            coupon_codes,
            EXPLODE_OUTER(items) AS li_item
        FROM carts
    ) t
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        created_at,
        coupon_codes,
        qty,
        price,
        promo,
        SIZE(coupon_codes) AS coupon_codes_count
    FROM lines
    WHERE promo IS NOT NULL
      AND created_at > DATE '2025-03-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        price,
        coupon_codes_count,
        ROUND(price / SUM(price) OVER (PARTITION BY promo), 4) AS price_share,
        PERCENT_RANK() OVER (PARTITION BY TO_DATE(created_at) ORDER BY coupon_codes_count DESC NULLS FIRST) AS coupon_codes_count_percent_rank
    FROM cleaned
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        price,
        coupon_codes_count,
        price_share,
        coupon_codes_count_percent_rank,
        CASE WHEN coupon_codes_count >= 63 THEN 'High' WHEN coupon_codes_count >= 26 THEN 'Medium' ELSE 'Low' END AS coupon_codes_count_tier
    FROM with_windows
    WHERE coupon_codes_count_percent_rank > 0.76
)
-- Multi-level aggregation
SELECT
    CASE WHEN price_share >= 0.72 THEN 'HIGH' ELSE 'LOW' END AS price_share_tier,
    APPROX_PERCENTILE(coupon_codes_count, 0.9) AS coupon_codes_count_p90,
    PERCENTILE_APPROX(price_share, 0.5) AS price_share_p50,
    MIN(price) AS min_price,
    APPROX_PERCENTILE(price_share, ARRAY(0.25, 0.5, 0.75)) AS price_share_quartiles
FROM base
GROUP BY CASE WHEN price_share >= 0.72 THEN 'HIGH' ELSE 'LOW' END
ORDER BY price_share_tier DESC NULLS LAST;
