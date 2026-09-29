WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        c.id,
        c.created_at,
        rec.product_id,
        rec.qty,
        rec.price,
        rec.promo
    FROM carts c
    LATERAL VIEW EXPLODE(c.items) t AS rec
),
enriched AS (
    -- Running and moving aggregates
    SELECT *, MAX(price) OVER (PARTITION BY promo) promo_max_price
    FROM lines
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        price,
        promo,
        TRIM(promo) clean_promo
    FROM enriched
    WHERE created_at > DATE '2025-03-01'
      AND promo_max_price > 235
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(price) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) price_max_2h,
        COUNT(price) OVER w_recent price_count_2h
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    CASE WHEN price_count_2h >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_count_2h_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(price), 2) AS avg_price,
    SUM(price_max_2h) AS total_price_max_2h,
    SIZE(COLLECT_SET(promo)) AS num_promos,
    GROUPING_ID() AS grouping_level
FROM metrics
GROUP BY GROUPING SETS ((DATE_TRUNC('DAY', created_at), CASE WHEN price_count_2h >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('DAY', created_at)));
