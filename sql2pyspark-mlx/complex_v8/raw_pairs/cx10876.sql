WITH base AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE unit_price >= 130.6
),
summarized_rows AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MIN(st.unit_price) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS min_move_unit_price,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT MIN(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS first_moved_at
    FROM base s
)
-- Subtotals with CUBE
SELECT
    move_type,
    warehouse_id,
    SUM(days_since_moved_at) AS total_days_since_moved_at,
    MAX(moved_at) AS last_moved_at_2,
    COUNT(DISTINCT move_type) AS distinct_move_types
FROM summarized_rows
GROUP BY CUBE(move_type, warehouse_id)
