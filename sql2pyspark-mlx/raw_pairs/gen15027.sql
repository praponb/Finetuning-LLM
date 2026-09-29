WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        INLINE(items)
    FROM carts
),
scored AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        qty,
        price,
        promo,
        ROUND(qty / SUM(qty) OVER (PARTITION BY customer_id), 4) qty_share,
        MAX(product_id) OVER (PARTITION BY DATE_TRUNC('MONTH', created_at), promo) created_at_period_max_product_id,
        AVG(product_id) OVER (PARTITION BY TO_DATE(created_at), promo) created_at_to_date_avg_product_id
    FROM lines
),
scored_carts AS (
    SELECT
        id,
        created_at,
        price,
        promo,
        qty_share,
        created_at_period_max_product_id,
        created_at_to_date_avg_product_id,
        ROUND(created_at_to_date_avg_product_id / SUM(created_at_to_date_avg_product_id) OVER (PARTITION BY promo), 4) created_at_to_date_avg_product_id_share,
        MAX(created_at_to_date_avg_product_id) OVER w_running created_at_to_date_avg_product_id_max_30m,
        DENSE_RANK() OVER w_recent created_at_period_max_product_id_dense_rank,
        CUME_DIST() OVER w_running_2 qty_share_cume_dist
    FROM scored
    WINDOW w_running AS (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY created_at_period_max_product_id DESC NULLS FIRST), w_running_2 AS (ORDER BY qty_share DESC NULLS FIRST)
),
scored_carts_2 AS (
    SELECT
        created_at,
        qty_share,
        created_at_to_date_avg_product_id_max_30m,
        created_at_period_max_product_id_dense_rank,
        qty_share_cume_dist,
        MAX(created_at_to_date_avg_product_id_share) OVER (PARTITION BY promo ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) created_at_to_date_avg_product_id_share_max_15m,
        COUNT(qty_share) OVER w_part qty_share_count_30m,
        MAX(created_at_period_max_product_id_dense_rank) OVER w_grp created_at_period_max_product_id_dense_rank_max_1h
    FROM scored_carts
    WHERE created_at_period_max_product_id_dense_rank IS NOT NULL
    WINDOW w_part AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW), w_grp AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW)
),
enriched AS (
    SELECT
        *,
        COUNT(qty_share_count_30m) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) qty_share_count_30m_count_2h,
        DENSE_RANK() OVER (ORDER BY created_at_to_date_avg_product_id_share_max_15m NULLS FIRST) created_at_to_date_avg_product_id_share_max_15m_dense_rank,
        COUNT(qty_share_count_30m) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) qty_share_count_30m_count_1h
    FROM scored_carts_2
)
SELECT
    created_at,
    qty_share,
    created_at_period_max_product_id_dense_rank,
    qty_share_cume_dist,
    created_at_to_date_avg_product_id_share_max_15m,
    qty_share_count_30m,
    created_at_period_max_product_id_dense_rank_max_1h,
    qty_share_count_30m_count_2h,
    created_at_to_date_avg_product_id_share_max_15m_dense_rank
FROM enriched
ORDER BY created_at_period_max_product_id_dense_rank_max_1h DESC
