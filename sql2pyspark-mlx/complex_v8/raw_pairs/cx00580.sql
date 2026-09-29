WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        LEAST(qty, unit_price) AS least_qty_unit_price,
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_label,
        moved_at + INTERVAL 1 DAYS AS moved_at_plus_1d
    FROM stock_moves
    WHERE operator <> 'op_dew'
),
screened AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM base_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price > 58.6
        )
),
filtered AS (
    -- Keep rows whose warehouse matches
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM warehouses w
            WHERE w.id = s.warehouse_id
              AND LOWER(w.wh_code) LIKE '%nx%'
              AND w.capacity >= 4912
        )
),
enriched_warehouse AS (
    -- Join warehouses
    SELECT
        s.*,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        sk.category AS sku_category,
        sk.labels AS sku_labels
    FROM filtered s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT
    sku_id,
    operator,
    warehouse_city,
    GROUPING_ID() AS gid,
    MIN(moved_at_plus_1d) AS first_moved_at_plus_1d
FROM enriched_warehouse
GROUP BY ROLLUP(sku_id, operator, warehouse_city)
ORDER BY sku_id NULLS LAST, operator NULLS LAST, warehouse_city NULLS LAST
