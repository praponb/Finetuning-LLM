WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        ROUND(unit_price * 1.1, 2) AS unit_price_adj,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix
    FROM stock_moves
    WHERE move_type NOT IN ('INBOUND', 'OUTBOUND')
      AND unit_price >= 130.6
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at
    FROM filtered s
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(qty) OVER (PARTITION BY move_count) AS move_count_max_qty,
        MIN(unit_price_adj) OVER (PARTITION BY move_count) AS move_count_min_unit_price_adj,
        DENSE_RANK() OVER (ORDER BY qty DESC) AS qty_dense_rank,
        ROUND(AVG(qty) OVER (ORDER BY qty DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS qty_avg_2_prior,
        MAX(qty) OVER (ORDER BY qty DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS qty_max_1_prior
    FROM with_counts
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER w_warehouse_id_sku_id_by_move AS move_count_max_qty_decile,
        ROUND(AVG(qty_max_1_prior) OVER w_move_count_sku_id, 2) AS move_count_sku_id_avg_qty_max_1_prior,
        ROUND(qty - AVG(qty) OVER w_move_count_sku_id, 2) AS qty_vs_move_count_sku_id_avg,
        MIN(unit_price_adj) OVER (PARTITION BY move_count, sku_id) AS move_count_sku_id_min_unit_price_adj,
        qty - LAG(qty) OVER (PARTITION BY warehouse_id, sku_id ORDER BY move_count_max_qty NULLS LAST, id) AS qty_change,
        COUNT(*) OVER (PARTITION BY move_count, sku_id) AS rows_in_move_count_sku_id
    FROM with_windows
    WINDOW w_warehouse_id_sku_id_by_move AS (PARTITION BY warehouse_id, sku_id ORDER BY move_count_max_qty NULLS LAST, id), w_move_count_sku_id AS (PARTITION BY move_count, sku_id)
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        move_type_slug,
        unit_price_adj,
        move_type_prefix,
        move_count,
        first_moved_at,
        move_count_max_qty,
        move_count_min_unit_price_adj,
        qty_avg_2_prior,
        qty_max_1_prior,
        move_count_max_qty_decile,
        move_count_sku_id_avg_qty_max_1_prior,
        qty_vs_move_count_sku_id_avg,
        move_count_sku_id_min_unit_price_adj,
        qty_change,
        rows_in_move_count_sku_id
    FROM with_trends
    WHERE qty_dense_rank <= 5
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    qty,
    moved_at,
    unit_price,
    move_type_prefix,
    move_count,
    move_count_max_qty,
    move_count_min_unit_price_adj,
    qty_avg_2_prior,
    qty_max_1_prior,
    move_count_max_qty_decile,
    move_count_sku_id_avg_qty_max_1_prior,
    qty_vs_move_count_sku_id_avg,
    move_count_sku_id_min_unit_price_adj,
    rows_in_move_count_sku_id,
    DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at,
    ROUND(unit_price / NULLIF(unit_price_adj, 0), 3) AS unit_price_per_unit_price_adj
FROM top_rows
ORDER BY warehouse_id NULLS LAST
