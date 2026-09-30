WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        unit_price - qty AS unit_price_minus_qty,
        CONCAT_WS('|', operator, move_type) AS operator_move_type_label,
        CONCAT(move_type, '#', CAST(sku_id AS STRING)) AS move_type_ref
    FROM stock_moves
    WHERE qty <= 392
      AND moved_at >= TIMESTAMP '2025-01-13 09:00:00'
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.unit_price_minus_qty,
        s.operator_move_type_label,
        s.move_type_ref,
        sk.sku_code,
        sk.labels AS sku_labels,
        sk.category AS sku_category
    FROM recent s
    JOIN skus sk ON sk.id = s.sku_id
),
combined_details AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator_move_type_label,
        s.move_type_ref,
        s.sku_labels,
        s.sku_category,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on
    FROM with_parent s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
)
SELECT
    sku_category,
    warehouse_city,
    warehouse_id,
    GROUPING_ID() AS grouping_level,
    SUM(CASE WHEN UPPER(TRIM(warehouse_wh_code)) LIKE 'K%' THEN 1 ELSE 0 END) AS n_upper_trim_warehouse_wh_code,
    ARRAY_SORT(COLLECT_SET(sku_category)) AS sku_category_list
FROM combined_details
GROUP BY ROLLUP(sku_category, warehouse_city, warehouse_id)
ORDER BY sku_category NULLS LAST, warehouse_city NULLS LAST, warehouse_id NULLS LAST
