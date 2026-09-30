WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        created_at,
        product_id,
        qty,
        promo,
        EXPLODE(coupon_codes) AS element
    FROM exploded
),
enriched AS (
    -- Ranking and gap features
    SELECT
        qty,
        promo,
        element,
        AVG(qty) OVER (PARTITION BY element) AS element_avg_qty,
        CUME_DIST() OVER (PARTITION BY TO_DATE(created_at), promo ORDER BY product_id) AS product_id_cume_dist,
        PERCENT_RANK() OVER (ORDER BY qty NULLS FIRST) AS qty_percent_rank
    FROM lines
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        promo,
        element,
        element_avg_qty,
        qty_percent_rank,
        CONCAT(promo, ', ', element) AS promo_element,
        COALESCE(promo, 'N/A') AS promo_clean
    FROM enriched
    WHERE element IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        element,
        element_avg_qty,
        qty_percent_rank,
        promo_element,
        promo_clean,
        DENSE_RANK() OVER (PARTITION BY promo, element ORDER BY qty_percent_rank) AS qty_percent_rank_dense_rank
    FROM filtered
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        element,
        CASE WHEN qty_percent_rank >= 0.74 THEN 'LARGE' ELSE 'SMALL' END AS qty_percent_rank_tier,
        MIN(element_avg_qty) AS min_element_avg_qty,
        ROUND(SUM(qty_percent_rank_dense_rank) / NULLIF(COUNT(CASE WHEN element = 'BUNDLE' THEN 1 END), 0), 2) AS qty_percent_rank_dense_rank_per_bundle,
        COUNT_IF(element = 'VIP') AS vip_events
    FROM metrics
    GROUP BY CUBE (element, CASE WHEN qty_percent_rank >= 0.74 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    element,
    min_element_avg_qty,
    vip_events
FROM summary
ORDER BY vip_events;
