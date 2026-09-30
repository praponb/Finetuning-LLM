WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(st.unit_price), 2) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS avg_move_unit_price
    FROM recent s
)
SELECT
    move_type,
    warehouse_id,
    ROUND(SUM(unit_price) / NULLIF(SUM(qty), 0), 3) AS unit_price_to_qty_ratio,
    MAX(qty) AS max_qty
FROM with_counts
GROUP BY ROLLUP(move_type, warehouse_id)
ORDER BY move_type NULLS LAST, warehouse_id NULLS LAST
