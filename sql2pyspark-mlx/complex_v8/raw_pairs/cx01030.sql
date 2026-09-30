WITH valid AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        CONCAT(operator, '#', CAST(id AS STRING)) AS operator_ref
    FROM stock_moves
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(unit_price) OVER (PARTITION BY sku_id, move_type) AS sku_id_move_type_sum_unit_price,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id, move_type), 2) AS unit_price_vs_sku_id_move_type_avg,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER w_sku_id_move_type, 0), 4) AS unit_price_share_of_sku_id_move_type,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY sku_id, move_type)) AS distinct_operator_in_sku_id_move_type,
        LEAD(moved_at) OVER (PARTITION BY move_type ORDER BY moved_at ASC, id) AS next_moved_at,
        COUNT(*) OVER w_sku_id_move_type AS rows_in_sku_id_move_type,
        LAST_VALUE(moved_at) OVER (ORDER BY moved_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_moved_at,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type ORDER BY moved_at ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_1_prior
    FROM valid
    WINDOW w_sku_id_move_type AS (PARTITION BY sku_id, move_type)
)
SELECT
    id,
    sku_id,
    move_type,
    moved_at,
    unit_price,
    sku_id_move_type_sum_unit_price,
    unit_price_vs_sku_id_move_type_avg,
    unit_price_share_of_sku_id_move_type,
    distinct_operator_in_sku_id_move_type,
    next_moved_at,
    rows_in_sku_id_move_type,
    last_moved_at,
    unit_price_avg_1_prior
FROM metrics
ORDER BY unit_price_share_of_sku_id_move_type DESC, unit_price DESC
