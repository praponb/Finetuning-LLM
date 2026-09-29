WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        CONCAT(move_type, '#', CAST(qty AS STRING)) AS move_type_ref,
        CASE WHEN unit_price >= 330.9 THEN 'A' WHEN unit_price >= 189.4 THEN 'B' ELSE 'C' END AS unit_price_tier
    FROM stock_moves
    WHERE warehouse_id >= 5
      AND unit_price BETWEEN 125.9 AND 387.1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        move_type_ref,
        COUNT(*) OVER (PARTITION BY unit_price_tier ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SUM(qty) OVER w_unit_price_tier_sku_id AS unit_price_tier_sku_id_sum_qty,
        DENSE_RANK() OVER (PARTITION BY sku_id, move_type ORDER BY moved_at DESC) AS moved_at_dense_rank,
        LEAD(qty) OVER (PARTITION BY unit_price_tier ORDER BY moved_at, id) AS next_qty,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at DESC), 3) AS moved_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY unit_price_tier ORDER BY moved_at), 3) AS moved_at_cume_dist,
        ROUND(AVG(qty) OVER (PARTITION BY unit_price_tier, sku_id), 2) AS unit_price_tier_sku_id_avg_qty,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS qty_avg_2,
        ROUND(qty - AVG(qty) OVER (PARTITION BY sku_id), 2) AS qty_vs_sku_id_avg,
        LEAD(moved_at) OVER (PARTITION BY unit_price_tier ORDER BY moved_at, id) AS next_moved_at,
        ROUND(qty - AVG(qty) OVER w_unit_price_tier_sku_id, 2) AS qty_vs_unit_price_tier_sku_id_avg,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(LAG(moved_at) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at DESC, id))) / 60, 1) AS minutes_since_prev_moved_at,
        COUNT(*) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_2
    FROM base_moves
    WINDOW w_unit_price_tier_sku_id AS (PARTITION BY unit_price_tier, sku_id)
),
totals AS (
    -- Group-level totals
    SELECT
        sku_id,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        MIN(next_moved_at) AS first_next_moved_at,
        COUNT(*) AS n_rows,
        ROUND(SUM(qty_vs_sku_id_avg) / NULLIF(SUM(moved_at_percent_rank), 0), 3) AS qty_vs_sku_id_avg_to_moved_at_percent_rank_ratio
    FROM with_windows
    WHERE qty_avg_2 > 125.8
    GROUP BY sku_id
    HAVING SUM(qty_avg_2) > 125.8
)
-- Final projection
SELECT
    sku_id,
    distinct_sku_ids,
    first_next_moved_at,
    qty_vs_sku_id_avg_to_moved_at_percent_rank_ratio,
    COALESCE(distinct_sku_ids, 0) AS distinct_sku_ids_filled
FROM totals
