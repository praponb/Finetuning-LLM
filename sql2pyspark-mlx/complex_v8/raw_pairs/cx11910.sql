WITH cleaned AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE MONTH(moved_at) IN (2, 3)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.unit_price,
        w.wh_code AS warehouse_wh_code,
        w.city AS warehouse_city,
        sk.labels AS sku_labels,
        sk.weight_kg AS sku_weight_kg
    FROM cleaned s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
    WHERE w.wh_code IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        warehouse_wh_code,
        warehouse_city,
        sku_labels,
        FIRST_VALUE(sku_weight_kg) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY sku_weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sku_weight_kg,
        unit_price - LAG(unit_price) OVER w_sku_id_by_qty AS unit_price_change,
        ROW_NUMBER() OVER w_sku_id_warehouse_city_by_sku AS sku_weight_kg_seq,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type ORDER BY unit_price DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_6_prior,
        LEAD(unit_price, 2) OVER (PARTITION BY move_type ORDER BY sku_weight_kg, id) AS next2_unit_price,
        LAST_VALUE(sku_weight_kg) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_sku_weight_kg,
        FIRST_VALUE(qty) OVER (PARTITION BY move_type, sku_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty,
        FIRST_VALUE(sku_weight_kg) OVER (PARTITION BY move_type, sku_id ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_sku_weight_kg,
        COUNT(*) OVER (PARTITION BY warehouse_id) AS rows_in_warehouse_id,
        ROUND(qty / NULLIF(SUM(qty) OVER (PARTITION BY warehouse_city, warehouse_id), 0), 4) AS qty_share_of_warehouse_city_warehouse_id,
        ROW_NUMBER() OVER w_move_type_sku_id_by_qty AS qty_seq,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_warehouse_city_warehouse_id, 0), 4) AS unit_price_share_of_warehouse_city_warehouse_id,
        ROUND(CUME_DIST() OVER (PARTITION BY move_type ORDER BY unit_price DESC, id), 3) AS unit_price_cume_dist,
        RANK() OVER (PARTITION BY move_type ORDER BY sku_weight_kg) AS sku_weight_kg_rank
    FROM combined_details
    WINDOW w_sku_id_by_qty AS (PARTITION BY sku_id ORDER BY qty, id), w_sku_id_warehouse_city_by_sku AS (PARTITION BY sku_id, warehouse_city ORDER BY sku_weight_kg DESC, id), w_move_type_sku_id_by_qty AS (PARTITION BY move_type, sku_id ORDER BY qty, id), w_warehouse_id AS (PARTITION BY warehouse_id), w_warehouse_city_warehouse_id AS (PARTITION BY warehouse_city, warehouse_id)
)
-- Final projection
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    unit_price,
    warehouse_wh_code,
    warehouse_city,
    sku_labels,
    unit_price_change,
    sku_weight_kg_seq,
    unit_price_avg_6_prior,
    last_known_sku_weight_kg,
    first_qty,
    first_sku_weight_kg,
    rows_in_warehouse_id,
    qty_share_of_warehouse_city_warehouse_id,
    qty_seq,
    unit_price_share_of_warehouse_city_warehouse_id,
    unit_price_cume_dist,
    ARRAY_CONTAINS(sku_labels, 'hazmat') AS has_hazmat
FROM windowed
ORDER BY first_sku_weight_kg DESC, unit_price_change
