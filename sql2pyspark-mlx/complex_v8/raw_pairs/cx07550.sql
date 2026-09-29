WITH recent_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM stock_moves
    WHERE unit_price < 399.6
),
joined_warehouse AS (
    -- Join warehouses
    SELECT
        s.*,
        w.wh_code AS warehouse_wh_code,
        w.opened_on AS warehouse_opened_on,
        w.city AS warehouse_city,
        sk.weight_kg AS sku_weight_kg,
        sk.labels AS sku_labels
    FROM recent_moves s
    JOIN warehouses w ON w.id = s.warehouse_id
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(least_qty_unit_price) OVER (PARTITION BY move_type), 2) AS move_type_avg_least_qty_unit_price,
        ROW_NUMBER() OVER (PARTITION BY sku_id, operator ORDER BY warehouse_opened_on, id) AS rn,
        ROUND(AVG(least_qty_unit_price) OVER (PARTITION BY warehouse_city ORDER BY qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS least_qty_unit_price_avg_7,
        ROW_NUMBER() OVER (PARTITION BY warehouse_city ORDER BY qty, id) AS seq_no,
        ROUND(AVG(least_qty_unit_price) OVER w_operator_sku_id, 2) AS operator_sku_id_avg_least_qty_unit_price,
        NTH_VALUE(warehouse_opened_on, 2) OVER (PARTITION BY warehouse_city, YEAR(moved_at) ORDER BY warehouse_opened_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_warehouse_opened_on,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type,
        MAX(qty) OVER (PARTITION BY sku_id, operator ORDER BY warehouse_opened_on, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS qty_max_2,
        ROUND(AVG(qty) OVER (PARTITION BY warehouse_city ORDER BY qty, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS qty_avg_2,
        ROUND(qty - AVG(qty) OVER (PARTITION BY operator, sku_id), 2) AS qty_vs_operator_sku_id_avg
    FROM joined_warehouse
    WINDOW w_operator_sku_id AS (PARTITION BY operator, sku_id), w_move_type AS (PARTITION BY move_type)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(least_qty_unit_price_avg_7) IGNORE NULLS OVER (PARTITION BY operator ORDER BY move_type_avg_least_qty_unit_price DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_least_qty_unit_price_avg_7,
        MAX(qty_vs_operator_sku_id_avg) OVER (ORDER BY sku_weight_kg NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS qty_vs_operator_sku_id_avg_max_4,
        MAX(least_qty_unit_price_avg_7) OVER (PARTITION BY warehouse_city) AS warehouse_city_max_least_qty_unit_price_avg_7
    FROM with_trends
)
SELECT
    id,
    warehouse_id,
    sku_id,
    move_type,
    qty,
    moved_at,
    operator,
    least_qty_unit_price,
    warehouse_opened_on,
    warehouse_city,
    move_type_avg_least_qty_unit_price,
    rn,
    least_qty_unit_price_avg_7,
    seq_no,
    operator_sku_id_avg_least_qty_unit_price,
    rows_in_move_type,
    qty_max_2,
    last_known_least_qty_unit_price_avg_7,
    qty_vs_operator_sku_id_avg_max_4,
    warehouse_city_max_least_qty_unit_price_avg_7
FROM windowed
