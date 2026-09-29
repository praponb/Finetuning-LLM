WITH prepared_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        unit_price
    FROM stock_moves
    WHERE qty > 169
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        unit_price,
        SUM(unit_price) OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS unit_price_sum_4,
        SIZE(COLLECT_SET(sku_id) OVER (PARTITION BY move_type)) AS distinct_sku_id_in_move_type,
        SUM(unit_price) OVER (PARTITION BY warehouse_id) AS warehouse_id_sum_unit_price,
        NTH_VALUE(moved_at, 2) OVER (PARTITION BY sku_id ORDER BY unit_price ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_moved_at,
        SUM(unit_price) OVER (PARTITION BY move_type) AS move_type_sum_unit_price,
        RANK() OVER (PARTITION BY move_type ORDER BY unit_price NULLS LAST) AS unit_price_rank,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (PARTITION BY sku_id ORDER BY unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price,
        SIZE(COLLECT_SET(warehouse_id) OVER w_move_type) AS distinct_warehouse_id_in_move_type,
        ROUND(AVG(unit_price) OVER (PARTITION BY move_type), 2) AS move_type_avg_unit_price
    FROM prepared_moves
    WINDOW w_move_type AS (PARTITION BY move_type)
),
grouped AS (
    -- Group-level totals
    SELECT
        warehouse_id,
        sku_id,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        SUM(last_known_unit_price) FILTER (WHERE move_type = 'OUTBOUND') AS last_known_unit_price_move_type_outbound,
        ROUND(AVG(unit_price_sum_4), 2) AS avg_unit_price_sum_4,
        COUNT(*) AS n_rows,
        MIN(second_moved_at) AS first_second_moved_at,
        ROUND(AVG(distinct_sku_id_in_move_type), 2) AS avg_distinct_sku_id_in_move_type
    FROM with_trends
    WHERE distinct_warehouse_id_in_move_type BETWEEN 4 AND 9
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    warehouse_id,
    n_rows,
    first_second_moved_at,
    avg_distinct_sku_id_in_move_type,
    ARRAY_JOIN(ARRAY_SORT(move_type_list), ',') AS move_type_list_text
FROM grouped
WHERE warehouse_id >= 5
ORDER BY warehouse_id DESC
