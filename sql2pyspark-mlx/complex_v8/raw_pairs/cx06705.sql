WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        ABS(qty - 191) AS qty_distance,
        CASE WHEN move_type IN ('TRANSFER', 'OUTBOUND') THEN 'group_a' WHEN move_type = 'ADJUST' THEN 'group_b' ELSE 'other' END AS move_type_group,
        INITCAP(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE operator IS NOT NULL
),
deduped AS (
    -- Keep the latest row per operator, warehouse_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        qty_distance,
        move_type_group,
        operator_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY operator, warehouse_id ORDER BY qty_distance DESC, id DESC) AS row_num
        FROM cleaned_moves
    ) ranked
    WHERE row_num = 1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        qty_distance,
        move_type_group,
        operator_clean,
        ROUND(CUME_DIST() OVER (ORDER BY qty_distance), 3) AS qty_distance_cume_dist,
        qty - LAG(qty) OVER (ORDER BY qty_distance, id) AS qty_change,
        MAX(qty) OVER (ORDER BY qty_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_qty_so_far,
        LAST_VALUE(qty) OVER (ORDER BY qty_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_qty,
        SUM(unit_price) OVER (PARTITION BY move_type_group) AS move_type_group_sum_unit_price
    FROM deduped
),
combined_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.qty_distance,
        s.operator_clean,
        s.qty_change,
        s.last_qty,
        sk.weight_kg AS sku_weight_kg,
        sk.sku_code
    FROM with_windows s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE LENGTH(TRIM(sk.sku_code)) >= 7
)
-- Subtotals with CUBE
SELECT
    warehouse_id,
    sku_id,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list,
    ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
FROM combined_details_sku
GROUP BY CUBE(warehouse_id, sku_id)
ORDER BY warehouse_id NULLS LAST, sku_id NULLS LAST
