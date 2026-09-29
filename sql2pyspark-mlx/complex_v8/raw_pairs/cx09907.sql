WITH filtered AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        ROUND(unit_price * 0.9, 2) AS unit_price_adj
    FROM stock_moves
    WHERE operator IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(unit_price_adj) OVER (PARTITION BY sku_id, operator) AS sku_id_operator_min_unit_price_adj,
        LAG(unit_price_adj) OVER w_all_by_qty AS prev_unit_price_adj,
        MIN(qty) OVER (PARTITION BY TO_DATE(moved_at), sku_id) AS moved_at_to_date_sku_id_min_qty,
        MIN(qty) OVER (PARTITION BY sku_id, operator) AS sku_id_operator_min_qty,
        DENSE_RANK() OVER (PARTITION BY sku_id ORDER BY unit_price_adj, id) AS unit_price_adj_dense_rank,
        unit_price_adj - LAG(unit_price_adj) OVER (ORDER BY unit_price_adj DESC NULLS FIRST, id) AS unit_price_adj_change,
        SUM(unit_price_adj) OVER (PARTITION BY TO_DATE(moved_at), sku_id) AS moved_at_to_date_sku_id_sum_unit_price_adj,
        SUM(qty) OVER (ORDER BY unit_price_adj DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS qty_sum_7,
        COUNT(*) OVER (PARTITION BY sku_id, operator) AS rows_in_sku_id_operator,
        ROUND(PERCENT_RANK() OVER w_sku_id_by_unit_price_adj, 3) AS unit_price_adj_percent_rank,
        COUNT(*) OVER (ORDER BY unit_price_adj DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(qty) OVER (PARTITION BY sku_id ORDER BY unit_price_adj, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS qty_max_2_prior
    FROM filtered
    WINDOW w_all_by_qty AS (ORDER BY qty DESC NULLS FIRST, id), w_sku_id_by_unit_price_adj AS (PARTITION BY sku_id ORDER BY unit_price_adj, id)
),
top_per_group AS (
    -- Top 2 per group
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price_adj,
        sku_id_operator_min_unit_price_adj,
        prev_unit_price_adj,
        moved_at_to_date_sku_id_min_qty,
        sku_id_operator_min_qty,
        unit_price_adj_change,
        moved_at_to_date_sku_id_sum_unit_price_adj,
        qty_sum_7,
        rows_in_sku_id_operator,
        unit_price_adj_percent_rank,
        running_count,
        qty_max_2_prior
    FROM with_trends
    WHERE unit_price_adj_dense_rank <= 2
      AND running_count < 16
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(unit_price_adj - AVG(unit_price_adj) OVER (PARTITION BY TO_DATE(moved_at)), 2) AS unit_price_adj_vs_moved_at_to_date_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(moved_at), warehouse_id ORDER BY moved_at_to_date_sku_id_min_qty DESC), 3) AS moved_at_to_date_sku_id_min_qty_percent_rank,
        SUM(qty) OVER (PARTITION BY TO_DATE(moved_at)) AS moved_at_to_date_sum_qty,
        MIN(unit_price_adj) OVER (PARTITION BY TO_DATE(moved_at)) AS moved_at_to_date_min_unit_price_adj
    FROM top_per_group
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    moved_at,
    unit_price_adj,
    sku_id_operator_min_unit_price_adj,
    prev_unit_price_adj,
    moved_at_to_date_sku_id_min_qty,
    sku_id_operator_min_qty,
    unit_price_adj_change,
    qty_sum_7,
    rows_in_sku_id_operator,
    unit_price_adj_percent_rank,
    running_count,
    qty_max_2_prior,
    unit_price_adj_vs_moved_at_to_date_avg,
    moved_at_to_date_sku_id_min_qty_percent_rank,
    moved_at_to_date_sum_qty,
    moved_at_to_date_min_unit_price_adj,
    CONCAT_WS(': ', move_type, operator) AS move_type_operator_label
FROM with_windows
ORDER BY running_count, moved_at_to_date_min_unit_price_adj, id
LIMIT 10
