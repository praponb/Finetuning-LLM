WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        product_id,
        price,
        promo,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM exploded
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        created_at,
        product_id,
        price,
        promo,
        tag,
        ROUND(price / SUM(price) OVER (PARTITION BY tag), 4) AS price_share,
        MIN(price) OVER (PARTITION BY tag) AS tag_min_price,
        MAX(product_id) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS product_id_max_15m,
        RANK() OVER w_recent AS product_id_rank
    FROM flattened
    WHERE promo IS NOT NULL
    WINDOW w_recent AS (PARTITION BY tag, promo ORDER BY product_id NULLS FIRST)
),
with_windows_carts AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY tag, promo ORDER BY product_id_rank DESC NULLS LAST) AS product_id_rank_rank,
        SUM(product_id_rank) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS product_id_rank_sum_15m
    FROM with_windows
)
-- Grouped metrics
SELECT
    CASE WHEN tag_min_price >= 241 THEN 'LARGE' ELSE 'SMALL' END AS tag_min_price_tier,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    tag,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    COUNT(*) FILTER (WHERE tag = 'WELCOME5') AS welcome5_rows,
    SORT_ARRAY(COLLECT_SET(promo)) AS promo_list
FROM with_windows_carts
GROUP BY CASE WHEN tag_min_price >= 241 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('HOUR', created_at), tag
HAVING welcome5_rows > 1
ORDER BY tag DESC;
