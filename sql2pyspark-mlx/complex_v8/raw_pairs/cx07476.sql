WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
    WHERE warehouse_id >= 5
      AND sku_id <= 6
),
with_parent_warehouse AS (
    -- Attach warehouse attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.moved_at,
        s.operator,
        s.unit_price,
        w.city AS warehouse_city,
        w.wh_code AS warehouse_wh_code
    FROM filtered_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_so_far,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id, move_type), 2) AS unit_price_vs_sku_id_move_type_avg,
        COUNT(*) OVER (PARTITION BY sku_id, move_type) AS rows_in_sku_id_move_type,
        MIN(unit_price) OVER (ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_price_so_far_2,
        ROUND(AVG(unit_price) OVER (ORDER BY unit_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS unit_price_avg_7,
        ROUND(AVG(unit_price) OVER (PARTITION BY sku_id, move_type), 2) AS sku_id_move_type_avg_unit_price,
        SUM(unit_price) OVER (PARTITION BY sku_id ORDER BY unit_price DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS unit_price_sum_3,
        unit_price - LAG(unit_price) OVER w_all_by_unit_price AS unit_price_change,
        ROUND(CUME_DIST() OVER (ORDER BY unit_price DESC), 3) AS unit_price_cume_dist,
        unit_price - LAG(unit_price) OVER (PARTITION BY sku_id ORDER BY unit_price DESC, id) AS unit_price_change_2
    FROM with_parent_warehouse
    WINDOW w_sku_id_move_type AS (PARTITION BY sku_id, move_type), w_all_by_unit_price AS (ORDER BY unit_price, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(warehouse_wh_code, '#', CAST(warehouse_id AS STRING)) AS warehouse_wh_code_ref
    FROM ranked
)
SELECT
    id,
    sku_id,
    move_type,
    moved_at,
    operator,
    warehouse_city,
    warehouse_wh_code,
    min_unit_price_so_far,
    unit_price_vs_sku_id_move_type_avg,
    unit_price_avg_7,
    unit_price_sum_3,
    unit_price_cume_dist,
    unit_price_change_2,
    warehouse_wh_code_ref
FROM tagged
WHERE moved_at >= TIMESTAMP '2025-01-31 08:00:00'
