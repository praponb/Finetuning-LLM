WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND unit_price < 328.3
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        sk.unit_cost AS sku_unit_cost,
        sk.category AS sku_category
    FROM cleaned s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
line_level AS (
    -- One row per move
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        st.id AS move_id,
        st.moved_at AS move_moved_at,
        st.operator AS move_operator
    FROM combined_details s
    JOIN stock_moves st ON st.sku_id = s.sku_id
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        move_id,
        DATEDIFF(DATE '2026-01-01', moved_at) AS days_since_moved_at
    FROM line_level
)
SELECT
    id,
    move_type,
    moved_at,
    move_id,
    days_since_moved_at,
    DAYOFWEEK(moved_at) AS moved_at_dayofweek,
    HOUR(moved_at) AS moved_at_hour
FROM enriched
ORDER BY days_since_moved_at DESC NULLS FIRST, moved_at_dayofweek DESC
