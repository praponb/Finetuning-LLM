WITH base_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        CONCAT_WS(': ', move_type, operator) AS move_type_operator_label
    FROM stock_moves
),
per_group AS (
    -- Group-level totals
    SELECT
        sku_id,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        MAX(moved_at) AS last_moved_at,
        COUNT(*) AS row_count,
        COUNT(DISTINCT id) AS distinct_ids
    FROM base_moves
    GROUP BY sku_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        distinct_ids - LAG(distinct_ids) OVER (PARTITION BY sku_id, distinct_ids ORDER BY row_count, sku_id) AS distinct_ids_change,
        MAX(distinct_ids) OVER (PARTITION BY sku_id, distinct_ids ORDER BY row_count, sku_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS distinct_ids_max_6_prior
    FROM per_group
)
SELECT
    sku_id,
    move_type_list,
    operator_list,
    last_moved_at,
    row_count,
    distinct_ids_change
FROM with_trends
WHERE distinct_ids_change > -3
ORDER BY row_count DESC, distinct_ids_change
