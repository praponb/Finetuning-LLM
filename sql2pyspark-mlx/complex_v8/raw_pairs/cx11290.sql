WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE move_type IN ('ADJUST', 'OUTBOUND', 'INBOUND')
      AND MONTH(moved_at) IN (1, 2)
),
with_details_warehouse AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        w.wh_code AS warehouse_wh_code
    FROM valid_moves s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.capacity BETWEEN 2614 AND 11506
    WHERE LENGTH(TRIM(w.wh_code)) >= 4
),
profiled AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.sku_id = s.sku_id) AS last_moved_at
    FROM with_details_warehouse s
)
-- Final projection
SELECT
    id,
    operator,
    warehouse_wh_code,
    last_moved_at,
    ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(last_moved_at)) / 3600, 1) AS hours_last_moved_at_to_moved_at
FROM profiled
WHERE sku_id >= 16
ORDER BY last_moved_at, hours_last_moved_at_to_moved_at, id
LIMIT 10
