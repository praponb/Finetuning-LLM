WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        DATE_FORMAT(moved_at, 'MMM yyyy') AS moved_at_label,
        CONCAT_WS(': ', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE operator IN ('op_bee', 'op_eve', 'op_chan')
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(unit_price - 293.2) AS unit_price_distance,
        LOWER(TRIM(operator_move_type_label)) AS operator_move_type_label_clean,
        unit_price + qty AS unit_price_plus_qty
    FROM cleaned_moves
),
joined_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.operator,
        s.unit_price,
        s.moved_at_label,
        s.operator_move_type_label_clean,
        s.unit_price_plus_qty,
        sk.unit_cost AS sku_unit_cost
    FROM tagged s
    JOIN skus sk ON sk.id = s.sku_id AND UPPER(TRIM(sk.sku_code)) LIKE 'A%'
    WHERE sk.unit_cost < 302.5
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS move_count,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at,
        (SELECT SUM(st.unit_price) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS total_move_unit_price
    FROM joined_sku s
)
SELECT
    id,
    sku_id,
    move_type,
    qty,
    operator,
    unit_price,
    moved_at_label,
    unit_price_plus_qty,
    last_moved_at,
    total_move_unit_price,
    DATE_FORMAT(last_moved_at, 'yyyy-MM') AS last_moved_at_label
FROM profiled
ORDER BY operator
