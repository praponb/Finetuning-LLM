WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        elem.product_id,
        elem.qty,
        elem.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) x AS elem
),
with_windows AS (
    SELECT
        *,
        AVG(qty) OVER w_ordered AS promo_avg_qty,
        ROUND(qty / SUM(qty) OVER (PARTITION BY promo), 4) AS qty_share,
        MAX(product_id) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS product_id_max_30m
    FROM lines
    WINDOW w_ordered AS (PARTITION BY promo)
),
with_windows_carts AS (
    SELECT
        *,
        AVG(qty) OVER w_recent AS qty_avg_15m,
        MAX(qty) OVER (PARTITION BY MONTH(created_at) ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS qty_max_1h,
        RANK() OVER (PARTITION BY promo ORDER BY qty DESC NULLS FIRST) AS qty_rank,
        AVG(qty_share) OVER (PARTITION BY promo) AS promo_avg_qty_share
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY TO_DATE(created_at), promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW)
),
valid AS (
    SELECT
        *,
        LEAST(promo_avg_qty_share, qty_share) AS least_promo_avg_qty_share_qty_share,
        CASE WHEN product_id_max_30m >= 9 THEN 'High' WHEN product_id_max_30m >= 3 THEN 'Medium' ELSE 'Low' END AS product_id_max_30m_level
    FROM with_windows_carts
    WHERE created_at >= TO_DATE('2025-03-01')
),
with_windows_carts_2 AS (
    SELECT
        *,
        MAX(product_id_max_30m) OVER (PARTITION BY TO_DATE(created_at), promo) AS created_at_to_date_max_product_id_max_30m,
        AVG(promo_avg_qty_share) OVER w_part AS promo_avg_qty_share_avg_15m,
        MAX(promo_avg_qty_share) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS promo_avg_qty_share_max_2h,
        MIN(promo_avg_qty) OVER (PARTITION BY promo) AS promo_min_promo_avg_qty,
        PERCENT_RANK() OVER w_part_2 AS qty_rank_percent_rank
    FROM valid
    WHERE product_id BETWEEN 5 AND 9
    WINDOW w_part AS (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY TO_DATE(created_at), promo ORDER BY qty_rank DESC NULLS FIRST)
),
metrics AS (
    SELECT
        *,
        SUM(product_id_max_30m) OVER (PARTITION BY YEAR(created_at), promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS product_id_max_30m_sum_15m,
        RANK() OVER (PARTITION BY promo ORDER BY promo_min_promo_avg_qty DESC NULLS LAST) AS promo_min_promo_avg_qty_rank
    FROM with_windows_carts_2
)
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    CASE WHEN product_id_max_30m >= 10 THEN 'HIGH' ELSE 'LOW' END AS product_id_max_30m_tier,
    COUNT(*) AS cnt,
    SUM(CASE WHEN promo = 'VIP' THEN qty_share ELSE 0 END) AS vip_qty_share,
    COUNT(DISTINCT promo) AS distinct_promos
FROM metrics
GROUP BY DATE_TRUNC('DAY', created_at), CASE WHEN product_id_max_30m >= 10 THEN 'HIGH' ELSE 'LOW' END
HAVING distinct_promos >= 1
ORDER BY product_id_max_30m_tier DESC
