WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        li_item.price
    FROM carts
    LATERAL VIEW EXPLODE(items) li AS li_item
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(price, 0) AS price_filled,
        ROUND(price, 0) AS price_rounded
    FROM lines
    WHERE TO_DATE(created_at) >= '2023-07-20'
),
cleaned_carts AS (
    -- Basic cleansing and defaults
    SELECT
        created_at,
        price_rounded,
        ROUND(price_filled, 0) AS price_filled_rounded
    FROM cleaned
    WHERE created_at >= DATE '2025-03-01'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(price_rounded) OVER (PARTITION BY YEAR(created_at)) AS created_at_year_avg_price_rounded,
        CUME_DIST() OVER (ORDER BY price_rounded DESC NULLS LAST) AS price_rounded_cume_dist,
        AVG(price_filled_rounded) OVER (PARTITION BY MONTH(created_at) ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS price_filled_rounded_avg_30m
    FROM cleaned_carts
    WHERE price_rounded <= 36
),
cleaned_carts_2 AS (
    -- Basic cleansing and defaults
    SELECT
        price_rounded,
        created_at_year_avg_price_rounded,
        price_rounded_cume_dist,
        price_filled_rounded_avg_30m,
        QUARTER(created_at) AS created_at_quarter
    FROM enriched
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 15:00:00')
)
-- Multi-level aggregation
SELECT
    CASE WHEN price_rounded_cume_dist >= 0.83 THEN 'LARGE' ELSE 'SMALL' END AS price_rounded_cume_dist_tier,
    CASE WHEN created_at_year_avg_price_rounded >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END AS created_at_year_avg_price_rounded_tier,
    COUNT(*) AS cnt,
    MAX(created_at_quarter) AS max_created_at_quarter,
    ROUND(AVG(price_rounded), 2) AS avg_price_rounded,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(created_at_quarter, ARRAY(0.25, 0.5, 0.75)) AS created_at_quarter_quartiles
FROM cleaned_carts_2
GROUP BY price_rounded_cume_dist_tier, created_at_year_avg_price_rounded_tier
HAVING cnt > 1;
