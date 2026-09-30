WITH base AS (
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
        LENGTH(TRIM(move_type)) AS move_type_len,
        ROUND(qty / NULLIF(unit_price, 0), 3) AS qty_per_unit_price,
        YEAR(moved_at) AS moved_at_year
    FROM stock_moves
    WHERE moved_at >= TIMESTAMP '2025-01-13 13:00:00'
      AND warehouse_id IS NOT NULL
),
screened AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND w.opened_on IS NOT NULL
        )
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        s.move_type_len,
        s.moved_at_year,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city
    FROM screened s
    JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    warehouse_id,
    move_type_len,
    sku_id,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT sku_id) AS distinct_sku_ids,
    COUNT(DISTINCT move_type) AS distinct_move_types
FROM combined_details_warehouse
GROUP BY GROUPING SETS ((warehouse_id, move_type_len, sku_id), (warehouse_id), (move_type_len), ())
ORDER BY warehouse_id NULLS LAST, move_type_len NULLS LAST, sku_id NULLS LAST
