WITH cleaned AS (
    -- Scope the stock_moves rows
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
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label
    FROM stock_moves
    WHERE qty >= 102
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS move_count
    FROM cleaned s
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(unit_price) OVER (PARTITION BY move_type) AS move_type_max_unit_price,
        ROUND(CUME_DIST() OVER (PARTITION BY warehouse_id, TO_DATE(moved_at) ORDER BY qty DESC), 3) AS qty_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY warehouse_id, TO_DATE(moved_at) ORDER BY qty DESC), 3) AS qty_percent_rank,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type,
        ROW_NUMBER() OVER w_operator_by_moved_at AS seq_no
    FROM with_counts
    WINDOW w_operator_by_moved_at AS (PARTITION BY operator ORDER BY moved_at ASC, id)
),
joined AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.moved_at_label,
        s.move_type_operator_label,
        s.move_type_max_unit_price,
        s.qty_percent_rank,
        s.rows_in_move_type,
        s.seq_no,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        sk.sku_code,
        sk.unit_cost AS sku_unit_cost
    FROM with_windows s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.unit_price) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS max_move_unit_price,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at
    FROM joined s
),
move_stats_3 AS (
    -- Per-sku stock_moves metrics
    SELECT
        sku_id,
        COUNT(*) AS move_count,
        COUNT(*) FILTER (WHERE move_type IS NOT NULL) AS move_move_type_count
    FROM stock_moves
    GROUP BY sku_id
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count,
        COALESCE(st.move_move_type_count, 0) AS move_move_type_count
    FROM summarized_rows s
    LEFT JOIN move_stats_3 st ON st.sku_id = s.sku_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(qty_percent_rank / NULLIF(SUM(qty_percent_rank) OVER w_warehouse_opened_on_month_op, 0), 4) AS qty_percent_rank_share_of_warehouse_opened_on_mo,
        ROUND(AVG(qty_percent_rank) OVER (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on), operator), 2) AS warehouse_opened_on_month_operator_avg_qty_perce
    FROM profiled
    WINDOW w_warehouse_opened_on_month_op AS (PARTITION BY DATE_TRUNC('MONTH', warehouse_opened_on), operator)
)
SELECT
    id,
    move_type,
    qty,
    moved_at,
    operator,
    move_type_operator_label,
    qty_percent_rank,
    rows_in_move_type,
    seq_no,
    warehouse_opened_on,
    warehouse_city,
    sku_code,
    first_moved_at,
    move_count,
    qty_percent_rank_share_of_warehouse_opened_on_mo,
    warehouse_opened_on_month_operator_avg_qty_perce,
    DATE_TRUNC('WEEK', warehouse_opened_on) AS warehouse_opened_on_week,
    ABS(move_count - 5) AS move_count_distance
FROM metrics
ORDER BY first_moved_at, move_count DESC NULLS FIRST
