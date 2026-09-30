WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        LENGTH(TRIM(move_type)) AS move_type_len,
        qty + unit_price AS qty_plus_unit_price
    FROM stock_moves
    WHERE qty < 357
      AND move_type <> 'TRANSFER'
),
with_parent AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.move_type_len,
        s.qty_plus_unit_price,
        sk.unit_cost AS sku_unit_cost,
        sk.labels AS sku_labels,
        w.wh_code AS warehouse_wh_code
    FROM scoped_moves s
    JOIN skus sk ON sk.id = s.sku_id
    JOIN warehouses w ON w.id = s.warehouse_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        move_type_len,
        qty_plus_unit_price,
        sku_unit_cost,
        sku_labels,
        warehouse_wh_code,
        LAG(sku_unit_cost) OVER (PARTITION BY warehouse_id, move_type ORDER BY move_type_len, id) AS prev_sku_unit_cost,
        FIRST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY sku_unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        ROUND(sku_unit_cost / NULLIF(SUM(sku_unit_cost) OVER w_sku_id_move_type, 0), 4) AS sku_unit_cost_share_of_sku_id_move_type,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_sku_id_move_type, 0), 4) AS unit_price_share_of_sku_id_move_type,
        COUNT(*) OVER w_sku_id_move_type AS rows_in_sku_id_move_type,
        NTILE(4) OVER w_warehouse_id_move_type_by_mo AS move_type_len_quartile,
        LAG(unit_price) OVER (PARTITION BY move_type ORDER BY move_type_len, id) AS prev_unit_price,
        ROUND(qty_plus_unit_price / NULLIF(SUM(qty_plus_unit_price) OVER (PARTITION BY warehouse_id), 0), 4) AS qty_plus_unit_price_share_of_warehouse_id,
        FIRST_VALUE(qty) IGNORE NULLS OVER (PARTITION BY warehouse_id, move_type ORDER BY move_type_len, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_qty,
        DENSE_RANK() OVER w_warehouse_id_move_type_by_mo_3 AS move_type_len_dense_rank,
        NTILE(10) OVER (PARTITION BY move_type ORDER BY move_type_len, id) AS move_type_len_decile
    FROM with_parent
    WINDOW w_sku_id_move_type AS (PARTITION BY sku_id, move_type), w_warehouse_id_move_type_by_mo AS (PARTITION BY warehouse_id, move_type ORDER BY move_type_len, id), w_warehouse_id_move_type_by_mo_3 AS (PARTITION BY warehouse_id, move_type ORDER BY move_type_len)
)
-- Final projection
SELECT
    id,
    qty,
    move_type_len,
    qty_plus_unit_price,
    sku_labels,
    prev_sku_unit_cost,
    first_known_unit_price,
    sku_unit_cost_share_of_sku_id_move_type,
    unit_price_share_of_sku_id_move_type,
    rows_in_sku_id_move_type,
    prev_unit_price,
    qty_plus_unit_price_share_of_warehouse_id,
    move_type_len_dense_rank,
    move_type_len_decile,
    SUBSTRING(TRIM(warehouse_wh_code), 1, 2) AS warehouse_wh_code_prefix,
    ROUND(sku_unit_cost * 1.5, 2) AS sku_unit_cost_adj
FROM windowed
WHERE prev_unit_price <= 367.7
ORDER BY rows_in_sku_id_move_type DESC, id
LIMIT 5
