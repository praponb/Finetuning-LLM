WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT *
    FROM stock_moves
    WHERE sku_id IS NOT NULL
      AND move_type <> 'TRANSFER'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        w.capacity AS warehouse_capacity,
        sk.labels AS sku_labels
    FROM base_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.capacity > 4912
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        unit_price,
        sku_labels,
        LAG(warehouse_capacity) OVER (ORDER BY warehouse_capacity DESC, id) AS prev_warehouse_capacity,
        ROW_NUMBER() OVER (PARTITION BY move_type ORDER BY unit_price, id) AS rn,
        LAG(unit_price, 2) OVER w_all_by_unit_price AS prev2_unit_price,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at
    FROM with_details
    WINDOW w_all_by_unit_price AS (ORDER BY unit_price DESC, id)
)
-- Subtotals with GROUPING SETS
SELECT
    warehouse_id,
    sku_id,
    COUNT(*) AS cnt,
    ROUND(STDDEV(prev2_unit_price), 2) AS stddev_prev2_unit_price
FROM with_windows
GROUP BY GROUPING SETS ((warehouse_id, sku_id), (warehouse_id), (sku_id))
ORDER BY warehouse_id NULLS LAST, sku_id NULLS LAST
