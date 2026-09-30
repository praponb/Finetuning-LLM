WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        INLINE(items)
    FROM carts
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        product_id,
        qty,
        promo,
        value
    FROM flattened
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS value
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        value,
        qty * product_id qty_times_product_id
    FROM exploded
    WHERE promo IS NOT NULL
      AND customer_id = 15
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY qty_times_product_id DESC NULLS FIRST) qty_times_product_id_cume_dist,
        DENSE_RANK() OVER (PARTITION BY value ORDER BY product_id NULLS FIRST) product_id_dense_rank
    FROM filtered
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN product_id_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS product_id_dense_rank_tier,
        COUNT(CASE WHEN value = 'WELCOME5' THEN 1 END) AS welcome5_count,
        MAX(qty_times_product_id) FILTER (WHERE value = 'SPRING10') AS spring10_max_qty_times_product_id,
        COUNT(CASE WHEN value = 'VIP' THEN 1 END) AS vip_count
    FROM with_windows
    GROUP BY product_id_dense_rank_tier
)
SELECT product_id_dense_rank_tier, vip_count
FROM daily;
