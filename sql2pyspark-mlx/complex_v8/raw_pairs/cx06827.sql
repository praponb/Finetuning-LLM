WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE move_type IS NOT NULL
      AND qty <= 256
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id, operator ORDER BY moved_at DESC, id), 3) AS moved_at_percent_rank,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator, sku_id), 0), 4) AS unit_price_share_of_operator_sku_id,
        ROUND(days_since_moved_at / NULLIF(SUM(days_since_moved_at) OVER w_operator, 0), 4) AS days_since_moved_at_share_of_operator,
        unit_price - LAG(unit_price) OVER (ORDER BY qty DESC, id) AS unit_price_change,
        DENSE_RANK() OVER (PARTITION BY operator, YEAR(moved_at) ORDER BY days_since_moved_at ASC) AS days_since_moved_at_dense_rank,
        ROUND(AVG(unit_price) OVER w_operator_sku_id, 2) AS operator_sku_id_avg_unit_price,
        SUM(days_since_moved_at) OVER (PARTITION BY operator, sku_id) AS operator_sku_id_sum_days_since_moved_at,
        ROUND(days_since_moved_at - AVG(days_since_moved_at) OVER w_warehouse_id_move_type, 2) AS days_since_moved_at_vs_warehouse_id_move_type_av,
        LAG(days_since_moved_at, 3) OVER w_move_type_operator_by_moved AS prev3_days_since_moved_at,
        ROUND(CUME_DIST() OVER (ORDER BY qty DESC), 3) AS qty_cume_dist,
        ROUND(days_since_moved_at - AVG(days_since_moved_at) OVER (PARTITION BY operator, sku_id), 2) AS days_since_moved_at_vs_operator_sku_id_avg,
        LAG(days_since_moved_at) OVER w_all_by_qty AS prev_days_since_moved_at,
        ROUND(CUME_DIST() OVER w_operator_moved_at_year_by_da, 3) AS days_since_moved_at_cume_dist
    FROM prepared_moves
    WINDOW w_operator AS (PARTITION BY operator), w_operator_sku_id AS (PARTITION BY operator, sku_id), w_warehouse_id_move_type AS (PARTITION BY warehouse_id, move_type), w_move_type_operator_by_moved AS (PARTITION BY move_type, operator ORDER BY moved_at DESC, id), w_all_by_qty AS (ORDER BY qty DESC, id), w_operator_moved_at_year_by_da AS (PARTITION BY operator, YEAR(moved_at) ORDER BY days_since_moved_at ASC)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    moved_at,
    moved_at_label,
    days_since_moved_at,
    moved_at_percent_rank,
    days_since_moved_at_share_of_operator,
    unit_price_change,
    days_since_moved_at_dense_rank,
    operator_sku_id_sum_days_since_moved_at,
    days_since_moved_at_vs_warehouse_id_move_type_av,
    days_since_moved_at_cume_dist,
    CONCAT_WS('-', moved_at_label, move_type) AS moved_at_label_move_type_label
FROM with_trends
ORDER BY days_since_moved_at_dense_rank DESC
