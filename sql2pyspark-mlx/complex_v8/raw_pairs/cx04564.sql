WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        CONCAT(operator, '#', CAST(sku_id AS STRING)) AS operator_ref
    FROM stock_moves
    WHERE warehouse_id <= 5
      AND unit_price >= 215.4
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(unit_price) OVER (PARTITION BY move_type) AS move_type_max_unit_price,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY move_type, operator)) AS distinct_warehouse_id_in_move_type_operator,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type, operator), 2) AS unit_price_vs_move_type_operator_avg,
        COUNT(*) OVER (PARTITION BY move_type) AS rows_in_move_type,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY move_type), 2) AS unit_price_vs_move_type_avg,
        LAG(unit_price) OVER (PARTITION BY warehouse_id, YEAR(moved_at) ORDER BY moved_at DESC, id) AS prev_unit_price,
        DENSE_RANK() OVER (PARTITION BY warehouse_id ORDER BY moved_at ASC NULLS LAST) AS moved_at_dense_rank,
        RANK() OVER (ORDER BY unit_price ASC) AS unit_price_rank,
        COUNT(*) OVER (PARTITION BY warehouse_id) AS rows_in_warehouse_id,
        ROW_NUMBER() OVER (PARTITION BY sku_id, move_type ORDER BY moved_at DESC, id) AS moved_at_seq
    FROM recent
),
grouped AS (
    -- Summarise per group
    SELECT
        unit_price_rank,
        warehouse_id,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        SUM(CASE WHEN move_type = 'OUTBOUND' THEN 1 ELSE 0 END) AS n_move_type_outbound,
        COUNT(DISTINCT warehouse_id) AS distinct_warehouse_ids,
        MIN(move_type_max_unit_price) AS min_move_type_max_unit_price
    FROM sequenced
    GROUP BY unit_price_rank, warehouse_id
    HAVING COUNT(*) >= 3
)
SELECT
    unit_price_rank,
    warehouse_id,
    move_type_list,
    distinct_sku_ids,
    distinct_warehouse_ids,
    ROUND(distinct_sku_ids / NULLIF(distinct_warehouse_ids, 0), 3) AS distinct_sku_ids_per_distinct_warehouse_ids
FROM grouped
ORDER BY unit_price_rank, warehouse_id
LIMIT 5
