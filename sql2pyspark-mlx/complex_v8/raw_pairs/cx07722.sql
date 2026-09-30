WITH recent AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE unit_price >= 180.0
      AND moved_at < TIMESTAMP '2025-03-02 06:00:00'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY warehouse_id, move_type ORDER BY qty DESC) AS qty_rank,
        ROW_NUMBER() OVER (PARTITION BY warehouse_id, move_type ORDER BY qty DESC, id) AS rn,
        DENSE_RANK() OVER (PARTITION BY warehouse_id, move_type ORDER BY qty DESC) AS qty_dense_rank
    FROM recent
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        qty_rank,
        rn,
        qty_dense_rank,
        NTILE(10) OVER (ORDER BY qty, id) AS qty_decile,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY qty_dense_rank), 0), 4) AS qty_share_of_qty_dense_rank,
        ROUND(qty_rank - AVG(qty_rank) OVER w_qty_dense_rank, 2) AS qty_rank_vs_qty_dense_rank_avg,
        ROUND(CUME_DIST() OVER w_all_by_qty_ties, 3) AS qty_cume_dist,
        ROUND(unit_price - AVG(unit_price) OVER w_qty_dense_rank, 2) AS unit_price_vs_qty_dense_rank_avg
    FROM windowed
    WINDOW w_qty_dense_rank AS (PARTITION BY qty_dense_rank), w_all_by_qty_ties AS (ORDER BY qty)
)
-- Subtotals with ROLLUP
SELECT
    qty_dense_rank,
    qty_decile,
    GROUPING_ID() AS gid,
    ROUND(AVG(qty_cume_dist), 2) AS avg_qty_cume_dist,
    COUNT(DISTINCT sku_id) AS distinct_sku_ids,
    PERCENTILE_APPROX(qty_decile, 0.5) AS median_qty_decile
FROM metrics
GROUP BY ROLLUP(qty_dense_rank, qty_decile)
ORDER BY qty_dense_rank NULLS LAST, qty_decile NULLS LAST
