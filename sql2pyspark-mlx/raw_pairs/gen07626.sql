WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        INLINE(items)
    FROM carts
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(price) OVER (PARTITION BY promo) AS promo_stddev_samp_price,
        SUM(qty) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS qty_sum_1h,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY qty DESC NULLS FIRST) AS qty_dense_rank
    FROM flattened
    WHERE product_id <= 8
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY promo ORDER BY qty DESC) AS qty_rank,
        ROUND(promo_stddev_samp_price / SUM(promo_stddev_samp_price) OVER (PARTITION BY YEAR(created_at), promo), 4) AS promo_stddev_samp_price_share
    FROM with_windows
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(product_id / qty_sum_1h, 2) AS product_id_per_qty_sum_1h,
        COALESCE(promo, 'NONE') AS promo_filled
    FROM scored
    WHERE created_at > TO_TIMESTAMP('2025-03-01 22:00:00')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (PARTITION BY promo ORDER BY product_id NULLS LAST) AS product_id_rank,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY qty_sum_1h DESC NULLS LAST) AS qty_sum_1h_cume_dist,
        MAX(product_id) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS product_id_max_2h
    FROM base
),
metrics AS (
    -- Ranking and gap features
    SELECT *, COUNT(qty_dense_rank) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS qty_dense_rank_count_1h
    FROM enriched
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics
    EXCEPT ALL
    SELECT id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 8 AND 16
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.id = k.id
),
daily AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('DAY', created_at) AS activity_day,
        ROUND(AVG(qty_dense_rank), 2) AS avg_qty_dense_rank,
        (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
        ROUND(AVG(qty), 2) AS avg_qty,
        SUM(product_id_rank) FILTER (WHERE promo = 'VIP') AS vip_total_product_id_rank
    FROM verified
    GROUP BY DATE_TRUNC('DAY', created_at)
)
SELECT
    activity_day,
    avg_qty_dense_rank,
    duration_min,
    avg_qty,
    vip_total_product_id_rank
FROM daily
ORDER BY duration_min DESC NULLS FIRST;
