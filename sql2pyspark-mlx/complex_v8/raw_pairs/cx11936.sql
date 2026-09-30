WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE sku_id <= 4
      AND move_type = 'TRANSFER'
),
enriched AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        s.move_type_len,
        s.unit_price_filled,
        w.opened_on AS warehouse_opened_on,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code
    FROM valid s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    move_type_len,
    move_type,
    warehouse_id,
    GROUPING(move_type_len) AS is_total_move_type_len,
    COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids
FROM enriched
GROUP BY GROUPING SETS ((move_type_len, move_type, warehouse_id), (move_type_len))
ORDER BY move_type_len NULLS LAST, move_type NULLS LAST, warehouse_id NULLS LAST
