WITH base_moves AS (
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
        CONCAT_WS('|', operator, move_type) AS operator_move_type_label,
        ABS(unit_price - 236.5) AS unit_price_distance
    FROM stock_moves
    WHERE warehouse_id >= 2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(moved_at) IGNORE NULLS OVER (PARTITION BY operator ORDER BY unit_price_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_moved_at,
        COUNT(*) OVER (PARTITION BY warehouse_id) AS rows_in_warehouse_id,
        LEAD(unit_price_distance, 2) OVER (PARTITION BY operator ORDER BY moved_at DESC, id) AS next2_unit_price_distance,
        ROUND(unit_price_distance / NULLIF(SUM(unit_price_distance) OVER (PARTITION BY sku_id, DATE_TRUNC('MONTH', moved_at)), 0), 4) AS unit_price_distance_share_of_sku_id_moved_at_mon,
        MIN(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_min_unit_price,
        unit_price - LAG(unit_price) OVER w_operator_by_moved_at AS unit_price_change,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id, DATE_TRUNC('MONTH', moved_at))) AS distinct_warehouse_id_in_sku_id_moved_at_month,
        NTILE(4) OVER (ORDER BY unit_price_distance DESC, id) AS unit_price_distance_quartile
    FROM base_moves
    WINDOW w_operator_by_moved_at AS (PARTITION BY operator ORDER BY moved_at DESC, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        operator_move_type_label,
        unit_price_distance,
        last_known_moved_at,
        rows_in_warehouse_id,
        unit_price_distance_share_of_sku_id_moved_at_mon,
        unit_price_change,
        distinct_warehouse_id_in_sku_id_moved_at_month,
        unit_price_distance_quartile,
        SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix,
        LOWER(REGEXP_REPLACE(TRIM(operator_move_type_label), ' ', '_')) AS operator_move_type_label_slug
    FROM with_windows
    WHERE move_type = 'INBOUND'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(unit_price) OVER (PARTITION BY operator, move_type ORDER BY distinct_warehouse_id_in_sku_id_moved_at_month DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_unit_price,
        unit_price_distance - LAG(unit_price_distance) OVER (PARTITION BY warehouse_id, unit_price_distance_quartile ORDER BY moved_at ASC, id) AS unit_price_distance_change,
        ROUND(unit_price_change - AVG(unit_price_change) OVER (PARTITION BY operator), 2) AS unit_price_change_vs_operator_avg
    FROM tagged
)
SELECT
    id,
    warehouse_id,
    moved_at,
    operator,
    unit_price,
    operator_move_type_label,
    unit_price_distance,
    rows_in_warehouse_id,
    unit_price_distance_share_of_sku_id_moved_at_mon,
    distinct_warehouse_id_in_sku_id_moved_at_month,
    unit_price_distance_quartile,
    move_type_prefix,
    running_unit_price,
    unit_price_distance_change
FROM metrics
ORDER BY unit_price_distance_quartile DESC, id
LIMIT 20
