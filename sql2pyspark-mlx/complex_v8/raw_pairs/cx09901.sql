WITH recent_moves AS (
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
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        COALESCE(unit_price, 0.0) AS unit_price_filled,
        LENGTH(TRIM(operator)) AS operator_len
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
),
sku_id_stats AS (
    -- Avg qty per sku_id
    SELECT sku_id, AVG(qty) AS avg_qty
    FROM recent_moves
    GROUP BY sku_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_qty, ROUND(s.qty - g.avg_qty, 2) AS qty_minus_avg
    FROM recent_moves s
    JOIN sku_id_stats g ON g.sku_id = s.sku_id
    WHERE s.qty > g.avg_qty
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_slug,
        s.operator_len,
        s.avg_qty,
        s.qty_minus_avg,
        w.opened_on AS warehouse_opened_on
    FROM vs_group s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
detail_rows_moves AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        s.move_type_slug,
        s.avg_qty,
        s.warehouse_opened_on,
        st.id AS move_id,
        st.qty AS move_qty,
        st.unit_price AS move_unit_price
    FROM with_details s
    LEFT JOIN stock_moves st ON st.warehouse_id = s.warehouse_id
)
-- Subtotals with GROUPING SETS
SELECT
    move_id,
    COALESCE(operator, 'ALL') AS operator,
    sku_id,
    GROUPING_ID() AS gid,
    SUM(avg_qty) FILTER (WHERE move_type = 'TRANSFER') AS avg_qty_move_type_transfer,
    ROUND(100 * COUNT(CASE WHEN operator = 'op_chan' THEN 1 END) / COUNT(*), 1) AS pct_operator_op_chan
FROM detail_rows_moves
GROUP BY GROUPING SETS ((move_id, operator, sku_id), (move_id), (operator), ())
