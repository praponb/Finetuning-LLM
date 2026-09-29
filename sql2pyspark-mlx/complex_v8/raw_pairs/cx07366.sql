WITH filtered_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        DATEDIFF(DATE '2025-12-31', moved_at) AS days_since_moved_at,
        unit_price + qty AS unit_price_plus_qty,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE operator IN ('op_bee', 'op_dew')
),
with_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price,
        s.days_since_moved_at,
        s.unit_price_plus_qty,
        s.operator_move_type_label,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        sk.unit_cost AS sku_unit_cost
    FROM filtered_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    JOIN skus sk ON sk.id = s.sku_id
),
with_stats AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at,
        (SELECT MIN(st.qty) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS min_move_qty,
        (SELECT MAX(st.moved_at) FROM stock_moves st WHERE st.warehouse_id = s.warehouse_id) AS last_moved_at_2
    FROM with_details_warehouse s
),
with_parent AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.unit_price,
        s.days_since_moved_at,
        s.unit_price_plus_qty,
        s.operator_move_type_label,
        s.warehouse_wh_code,
        s.warehouse_opened_on,
        s.sku_unit_cost,
        s.last_moved_at,
        s.min_move_qty,
        s.last_moved_at_2,
        w.capacity AS warehouse_capacity,
        w.wh_code AS warehouse_wh_code_2,
        w.city AS warehouse_city
    FROM with_stats s
    JOIN warehouses w ON w.id = s.warehouse_id AND w.capacity BETWEEN 4912 AND 11506
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', warehouse_opened_on) AS warehouse_opened_on_month,
        warehouse_city,
        MIN(warehouse_opened_on) AS first_warehouse_opened_on,
        ROUND(AVG(qty), 2) AS avg_qty,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(warehouse_city)) AS warehouse_city_list
    FROM with_parent
    GROUP BY DATE_TRUNC('MONTH', warehouse_opened_on), warehouse_city
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        SUBSTRING(TRIM(warehouse_city), 1, 2) AS warehouse_city_prefix,
        LOWER(REGEXP_REPLACE(TRIM(warehouse_city), ' ', '_')) AS warehouse_city_slug
    FROM summary
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        warehouse_opened_on_month,
        warehouse_city,
        first_warehouse_opened_on,
        distinct_ids,
        warehouse_city_list,
        warehouse_city_prefix,
        ABS(avg_qty - 225.6) AS avg_qty_distance
    FROM enriched
)
SELECT
    warehouse_opened_on_month,
    warehouse_city,
    first_warehouse_opened_on,
    distinct_ids,
    warehouse_city_list,
    warehouse_city_prefix,
    avg_qty_distance,
    SUBSTRING(TRIM(warehouse_city_prefix), 1, 4) AS warehouse_city_prefix_prefix
FROM enriched_2
WHERE SIZE(warehouse_city_list) >= 1
ORDER BY warehouse_city_prefix_prefix, warehouse_opened_on_month NULLS LAST
