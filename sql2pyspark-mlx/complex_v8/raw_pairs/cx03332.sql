WITH prepared AS (
    -- Filter stock_moves
    SELECT *
    FROM stock_moves
    WHERE warehouse_id >= 5
      AND sku_id IS NOT NULL
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, sk.sku_code, sk.category AS sku_category
    FROM prepared s
    JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type, sku_id ORDER BY qty ASC, id), 3) AS qty_cume_dist,
        COUNT(*) OVER (PARTITION BY move_type, sku_id ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM combined_details
),
with_details AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.unit_price,
        s.sku_code,
        s.qty_cume_dist,
        s.running_count,
        w.city AS warehouse_city,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code
    FROM with_trends s
    JOIN warehouses w ON w.id = s.warehouse_id
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(warehouse_city, 'UNKNOWN') AS warehouse_city_or_unknown,
        LOWER(TRIM(sku_code)) AS sku_code_clean
    FROM with_details
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.qty < 288
              AND st.unit_price >= 209.4
        )
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    unit_price,
    qty_cume_dist,
    running_count,
    warehouse_capacity,
    warehouse_wh_code,
    warehouse_city_or_unknown
FROM with_activity
ORDER BY warehouse_id DESC, id DESC
LIMIT 10
