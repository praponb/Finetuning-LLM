WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        src.id,
        src.created_at,
        elem.product_id,
        elem.qty,
        elem.price,
        elem.promo
    FROM carts src
    LATERAL VIEW EXPLODE(src.items) li AS elem
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        qty,
        promo,
        ROUND(qty / price, 2) AS qty_per_price,
        COALESCE(promo, 'DIRECT') AS resolved_promo
    FROM flattened
    WHERE promo IS DISTINCT FROM 'WELCOME5'
      AND created_at >= DATE '2025-03-01'
),
scored AS (
    -- Ranking and gap features
    SELECT *, RANK() OVER (PARTITION BY promo ORDER BY qty DESC NULLS FIRST) AS qty_rank
    FROM valid
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        qty,
        qty_rank,
        DENSE_RANK() OVER (PARTITION BY promo ORDER BY qty_rank) AS qty_rank_dense_rank,
        CUME_DIST() OVER (PARTITION BY promo ORDER BY qty_per_price NULLS FIRST) AS qty_per_price_cume_dist,
        PERCENT_RANK() OVER (ORDER BY qty_per_price DESC NULLS LAST) AS qty_per_price_percent_rank,
        DENSE_RANK() OVER (ORDER BY qty_per_price NULLS FIRST) AS qty_per_price_dense_rank
    FROM scored
    WHERE qty_rank BETWEEN 5 AND 6
),
enriched AS (
    -- Ranking and gap features
    SELECT
        qty,
        qty_rank_dense_rank,
        qty_per_price_percent_rank,
        qty_per_price_dense_rank,
        CUME_DIST() OVER w_hist AS qty_per_price_cume_dist_cume_dist,
        RANK() OVER w_hist_2 AS qty_rank_dense_rank_rank,
        DENSE_RANK() OVER w_grp AS qty_per_price_dense_rank_dense_rank,
        RANK() OVER w_hist AS qty_per_price_cume_dist_rank
    FROM with_windows
    WINDOW w_hist AS (ORDER BY qty_per_price_cume_dist DESC NULLS FIRST), w_hist_2 AS (ORDER BY qty_rank_dense_rank DESC), w_grp AS (ORDER BY qty_per_price_dense_rank NULLS LAST)
),
valid_carts AS (
    -- Keep recent, valid rows
    SELECT *, qty_rank_dense_rank_rank + qty_per_price_percent_rank AS qty_rank_dense_rank_rank_plus_qty_per_price_percent_rank
    FROM enriched
    WHERE qty_per_price_cume_dist_rank = 5
)
SELECT
    qty_rank_dense_rank,
    qty_per_price_percent_rank,
    qty_per_price_dense_rank,
    qty_per_price_cume_dist_cume_dist,
    qty_per_price_dense_rank_dense_rank,
    qty_per_price_cume_dist_rank
FROM valid_carts
