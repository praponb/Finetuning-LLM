WITH filtered AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price,
        CASE WHEN operator IN ('op_dew', 'op_ann') THEN 'group_a' WHEN operator = 'op_chan' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE qty >= 123
),
combined_details_warehouse AS (
    -- Join warehouses
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.unit_price,
        s.operator_group,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code,
        w.capacity AS warehouse_capacity
    FROM filtered s
    LEFT JOIN warehouses w ON w.id = s.warehouse_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        operator_group,
        warehouse_city,
        warehouse_wh_code,
        warehouse_capacity,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator_group, warehouse_city), 0), 4) AS unit_price_share_of_operator_group_warehouse_cit,
        unit_price - LAG(unit_price) OVER (PARTITION BY warehouse_city ORDER BY warehouse_capacity, id) AS unit_price_change,
        ROUND(AVG(unit_price) OVER (PARTITION BY operator_group, warehouse_city), 2) AS operator_group_warehouse_city_avg_unit_price,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER (PARTITION BY operator_group, warehouse_city), 2) AS warehouse_capacity_vs_operator_group_warehouse_c,
        LEAD(unit_price, 3) OVER (ORDER BY moved_at, id) AS next3_unit_price,
        NTILE(4) OVER w_warehouse_city_by_warehouse AS warehouse_capacity_quartile,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id, move_type), 2) AS warehouse_id_move_type_avg_unit_price,
        ROUND(warehouse_capacity - AVG(warehouse_capacity) OVER (PARTITION BY warehouse_id, move_type), 2) AS warehouse_capacity_vs_warehouse_id_move_type_avg
    FROM combined_details_warehouse
    WINDOW w_warehouse_city_by_warehouse AS (PARTITION BY warehouse_city ORDER BY warehouse_capacity, id)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(next3_unit_price, unit_price_change) AS least_next3_unit_price_unit_price_change
    FROM windowed
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    warehouse_city,
    warehouse_wh_code,
    warehouse_capacity,
    unit_price_change,
    operator_group_warehouse_city_avg_unit_price,
    warehouse_capacity_vs_operator_group_warehouse_c,
    warehouse_capacity_quartile,
    warehouse_capacity_vs_warehouse_id_move_type_avg,
    least_next3_unit_price_unit_price_change,
    CONCAT(warehouse_city, '#', CAST(warehouse_capacity AS STRING)) AS warehouse_city_ref
FROM derived
