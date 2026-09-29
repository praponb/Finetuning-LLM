WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        operator,
        LOWER(TRIM(operator)) AS operator_clean,
        ABS(qty - 249) AS qty_distance,
        qty + unit_price AS qty_plus_unit_price
    FROM stock_moves
    WHERE sku_id <= 3
      AND HOUR(moved_at) BETWEEN 10 AND 12
),
combined_details AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.operator,
        s.qty_distance,
        sk.sku_code,
        sk.weight_kg AS sku_weight_kg,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity
    FROM cleaned_moves s
    JOIN skus sk ON sk.id = s.sku_id
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
grouped AS (
    -- Group-level totals
    SELECT
        warehouse_id,
        PERCENTILE_APPROX(warehouse_capacity, 0.5) AS median_warehouse_capacity,
        ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list
    FROM combined_details
    GROUP BY warehouse_id
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM grouped s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
      AND SIZE(s.sku_code_list) >= 1
),
without_matches AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
        )
)
SELECT
    warehouse_id,
    median_warehouse_capacity,
    ARRAY_JOIN(ARRAY_SORT(sku_code_list), ',') AS sku_code_list_text,
    ARRAY_JOIN(ARRAY_SORT(sku_code_list), ',') AS sku_code_list_text_2
FROM without_matches
ORDER BY median_warehouse_capacity DESC, warehouse_id NULLS LAST
LIMIT 20
