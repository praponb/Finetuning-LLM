WITH recent_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
with_details_sku AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.move_type,
        s.operator,
        s.unit_price,
        sk.unit_cost AS sku_unit_cost,
        sk.sku_code
    FROM recent_moves s
    JOIN skus sk ON sk.id = s.sku_id
    WHERE LENGTH(TRIM(sk.sku_code)) >= 7
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY move_type ORDER BY sku_unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        DENSE_RANK() OVER (PARTITION BY move_type ORDER BY sku_unit_cost ASC) AS sku_unit_cost_dense_rank,
        SIZE(COLLECT_SET(operator) OVER w_sku_id_move_type) AS distinct_operator_in_sku_id_move_type,
        RANK() OVER w_warehouse_id_by_sku_unit_cos AS sku_unit_cost_rank,
        MAX(sku_unit_cost) OVER (PARTITION BY warehouse_id) AS warehouse_id_max_sku_unit_cost,
        NTILE(4) OVER w_warehouse_id_by_sku_unit_cos_2 AS sku_unit_cost_quartile,
        COUNT(*) OVER (PARTITION BY sku_id, move_type) AS rows_in_sku_id_move_type
    FROM with_details_sku
    WINDOW w_sku_id_move_type AS (PARTITION BY sku_id, move_type), w_warehouse_id_by_sku_unit_cos AS (PARTITION BY warehouse_id ORDER BY sku_unit_cost DESC), w_warehouse_id_by_sku_unit_cos_2 AS (PARTITION BY warehouse_id ORDER BY sku_unit_cost DESC, id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE sku_unit_cost_rank = 1
)
SELECT
    id,
    sku_id,
    move_type,
    operator,
    unit_price,
    sku_unit_cost,
    sku_code,
    sku_unit_cost_dense_rank,
    distinct_operator_in_sku_id_move_type,
    warehouse_id_max_sku_unit_cost,
    sku_unit_cost_quartile,
    rows_in_sku_id_move_type,
    CONCAT_WS('|', move_type, operator) AS move_type_operator_label,
    SUBSTRING(TRIM(operator), 1, 4) AS operator_prefix
FROM leaders
WHERE sku_unit_cost_quartile <= 9
ORDER BY sku_code NULLS LAST, id
LIMIT 20
