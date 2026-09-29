WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        DAYOFWEEK(moved_at) AS moved_at_dayofweek
    FROM stock_moves
    WHERE TO_DATE(moved_at) < DATE '2025-02-12'
      AND unit_price < 399.6
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(moved_at_dayofweek - AVG(moved_at_dayofweek) OVER w_warehouse_id_move_type, 2) AS moved_at_dayofweek_vs_warehouse_id_move_type_avg,
        FIRST_VALUE(qty) OVER (ORDER BY qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty
    FROM filtered_moves
    WINDOW w_warehouse_id_move_type AS (PARTITION BY warehouse_id, move_type)
),
combined_details_sku AS (
    -- Attach sku attributes
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.qty,
        s.moved_at,
        s.moved_at_dayofweek,
        s.first_qty,
        sk.weight_kg AS sku_weight_kg,
        sk.is_active AS sku_is_active
    FROM metrics s
    JOIN skus sk ON sk.id = s.sku_id
)
SELECT *
FROM (
    SELECT sku_id, move_type, moved_at_dayofweek
    FROM combined_details_sku
) src
PIVOT (
    COUNT(moved_at_dayofweek) AS count_v, SUM(moved_at_dayofweek) AS sum_v
    FOR move_type IN ('INBOUND', 'TRANSFER')
)
ORDER BY sku_id
