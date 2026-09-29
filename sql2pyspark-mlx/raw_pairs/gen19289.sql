WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        li_item.product_id,
        li_item.qty,
        li_item.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS li_item
),
flattened_carts AS (
    -- Unnest the array into one row per element
    SELECT qty, EXPLODE(coupon_codes) AS tag
    FROM flattened
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(tag) AS tag_upper
    FROM flattened_carts
    WHERE tag IS NOT DISTINCT FROM 'WELCOME5'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        qty,
        tag,
        RANK() OVER (PARTITION BY tag ORDER BY qty) AS qty_rank,
        RANK() OVER (ORDER BY qty DESC NULLS LAST) AS qty_rank_2,
        ROUND(qty / SUM(qty) OVER (PARTITION BY tag), 4) AS qty_share,
        COUNT(qty) OVER (PARTITION BY tag) AS tag_count_qty,
        DENSE_RANK() OVER (PARTITION BY tag ORDER BY qty DESC) AS qty_dense_rank
    FROM base
    WHERE qty = 3
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY qty_rank_2 DESC NULLS LAST) AS qty_rank_2_cume_dist,
        CUME_DIST() OVER w_ordered AS qty_rank_cume_dist,
        DENSE_RANK() OVER w_part AS qty_rank_dense_rank,
        RANK() OVER (ORDER BY qty_rank DESC NULLS LAST) AS qty_rank_rank,
        PERCENT_RANK() OVER (ORDER BY qty_share DESC NULLS FIRST) AS qty_share_percent_rank
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY tag ORDER BY qty_rank DESC NULLS LAST), w_part AS (PARTITION BY tag ORDER BY qty_rank)
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN tag_count_qty >= 14 THEN 'HIGH' ELSE 'LOW' END AS tag_count_qty_tier,
        CASE WHEN qty_rank_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS qty_rank_rank_tier,
        tag,
        PERCENTILE_APPROX(qty_share, ARRAY(0.25, 0.5, 0.75)) AS qty_share_quartiles,
        ROUND(AVG(qty_share_percent_rank), 2) AS avg_qty_share_percent_rank,
        COUNT(CASE WHEN tag = 'SPRING10' THEN 1 END) AS spring10_count
    FROM enriched
    GROUP BY CASE WHEN tag_count_qty >= 14 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN qty_rank_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, tag
)
SELECT
    tag_count_qty_tier,
    qty_rank_rank_tier,
    tag,
    qty_share_quartiles,
    avg_qty_share_percent_rank
FROM stats
ORDER BY tag_count_qty_tier DESC NULLS LAST, qty_rank_rank_tier DESC
