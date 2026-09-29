WITH cleaned_moves AS (
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
        ROUND(unit_price * 0.9, 2) AS unit_price_adj,
        FLOOR(unit_price / 100) * 100 AS unit_price_bucket
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-12'
      AND unit_price <= 367.6
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM cleaned_moves s
    WHERE s.warehouse_id IN (
            SELECT st.warehouse_id
            FROM stock_moves st
        )
      AND s.unit_price > 99.1
),
move_stats AS (
    -- Per-sku stock_moves metrics
    SELECT sku_id, COUNT(*) AS move_count
    FROM stock_moves
    GROUP BY sku_id
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        COALESCE(st.move_count, 0) AS move_count
    FROM without_matches s
    LEFT JOIN move_stats st ON st.sku_id = s.sku_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        unit_price_bucket,
        move_count,
        LAG(unit_price_adj) OVER w_warehouse_id_move_type_by_un AS prev_unit_price_adj,
        ROUND(PERCENT_RANK() OVER (ORDER BY moved_at ASC), 3) AS moved_at_percent_rank,
        LEAD(unit_price_adj, 2, 0) OVER (ORDER BY moved_at ASC, id) AS next2_unit_price_adj,
        COUNT(*) OVER (PARTITION BY sku_id) AS rows_in_sku_id
    FROM summarized_rows
    WINDOW w_warehouse_id_move_type_by_un AS (PARTITION BY warehouse_id, move_type ORDER BY unit_price_bucket DESC NULLS FIRST, id)
),
filtered AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM windowed s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.opened_on < DATE '2025-07-03'
        )
)
-- Final projection
SELECT
    id,
    warehouse_id,
    qty,
    moved_at,
    operator,
    unit_price,
    unit_price_bucket,
    move_count,
    moved_at_percent_rank,
    next2_unit_price_adj,
    rows_in_sku_id,
    CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label,
    DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label
FROM filtered
