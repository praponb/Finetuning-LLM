WITH filtered AS (
    -- Filter stock_moves
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
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        move_type,
        operator,
        unit_price,
        NTILE(3) OVER (PARTITION BY YEAR(moved_at), move_type ORDER BY unit_price DESC, id) AS unit_price_tercile,
        LAG(moved_at, 2) OVER (ORDER BY moved_at, id) AS prev2_moved_at,
        LAG(moved_at, 2) OVER (PARTITION BY move_type, warehouse_id ORDER BY moved_at DESC, id) AS prev2_moved_at_2,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY TO_DATE(moved_at))) AS distinct_operator_in_moved_at_to_date,
        COUNT(*) OVER (PARTITION BY TO_DATE(moved_at)) AS rows_in_moved_at_to_date,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY TO_DATE(moved_at))) AS distinct_move_type_in_moved_at_to_date,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY warehouse_id, operator), 2) AS unit_price_vs_warehouse_id_operator_avg,
        DENSE_RANK() OVER (ORDER BY moved_at) AS moved_at_dense_rank,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY warehouse_id, operator), 0), 4) AS unit_price_share_of_warehouse_id_operator,
        MAX(unit_price) OVER (PARTITION BY YEAR(moved_at), move_type ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_so_far,
        FIRST_VALUE(moved_at) IGNORE NULLS OVER (ORDER BY moved_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_moved_at,
        LEAD(moved_at) OVER (PARTITION BY sku_id, move_type ORDER BY moved_at, id) AS next_moved_at,
        MAX(unit_price) OVER (PARTITION BY TO_DATE(moved_at)) AS moved_at_to_date_max_unit_price,
        MIN(unit_price) OVER w_moved_at_to_date AS moved_at_to_date_min_unit_price,
        RANK() OVER (PARTITION BY YEAR(moved_at), move_type ORDER BY unit_price DESC, id) AS unit_price_rank,
        MIN(unit_price) OVER w_warehouse_id_operator AS warehouse_id_operator_min_unit_price
    FROM filtered
    WINDOW w_moved_at_to_date AS (PARTITION BY TO_DATE(moved_at)), w_warehouse_id_operator AS (PARTITION BY warehouse_id, operator)
),
top_rows AS (
    -- Top 3 per group
    SELECT *
    FROM with_trends
    WHERE moved_at_dense_rank <= 3
)
SELECT
    id,
    operator,
    unit_price,
    unit_price_tercile,
    prev2_moved_at,
    prev2_moved_at_2,
    distinct_operator_in_moved_at_to_date,
    rows_in_moved_at_to_date,
    distinct_move_type_in_moved_at_to_date,
    unit_price_vs_warehouse_id_operator_avg,
    moved_at_dense_rank,
    max_unit_price_so_far,
    first_known_moved_at,
    next_moved_at,
    moved_at_to_date_max_unit_price,
    moved_at_to_date_min_unit_price,
    unit_price_rank,
    warehouse_id_operator_min_unit_price
FROM top_rows
ORDER BY moved_at_to_date_max_unit_price
