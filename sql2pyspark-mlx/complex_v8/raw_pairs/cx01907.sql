WITH filtered_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price
    FROM stock_moves
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        ROUND(unit_price - AVG(unit_price) OVER (PARTITION BY sku_id), 2) AS unit_price_vs_sku_id_avg,
        SIZE(COLLECT_SET(warehouse_id) OVER w_sku_id) AS distinct_warehouse_id_in_sku_id
    FROM filtered_moves
    WINDOW w_sku_id AS (PARTITION BY sku_id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(qty, 3) OVER (ORDER BY unit_price_vs_sku_id_avg, id) AS prev3_qty,
        LAG(qty) OVER w_all_by_unit_price_vs_sku_id AS prev_qty,
        COUNT(*) OVER (ORDER BY unit_price_vs_sku_id_avg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MAX(qty) OVER (ORDER BY unit_price_vs_sku_id_avg, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS qty_max_4,
        ROUND(unit_price / NULLIF(SUM(unit_price) OVER (PARTITION BY operator, move_type), 0), 4) AS unit_price_share_of_operator_move_type,
        SUM(unit_price_vs_sku_id_avg) OVER (PARTITION BY operator, move_type) AS operator_move_type_sum_unit_price_vs_sku_id_avg
    FROM ranked
    WINDOW w_all_by_unit_price_vs_sku_id AS (ORDER BY unit_price_vs_sku_id_avg, id)
),
rollup_base AS (
    -- Summarise per group
    SELECT
        move_type,
        CASE WHEN qty_max_4 >= 1050 THEN 'HIGH' WHEN qty_max_4 >= 376 THEN 'MEDIUM' ELSE 'LOW' END AS qty_max_4_tier,
        MIN(qty_max_4) AS min_qty_max_4,
        MAX(qty) AS max_qty,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list
    FROM sequenced
    GROUP BY move_type, CASE WHEN qty_max_4 >= 1050 THEN 'HIGH' WHEN qty_max_4 >= 376 THEN 'MEDIUM' ELSE 'LOW' END
    HAVING AVG(running_count) > 9
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        SUM(max_qty) OVER (PARTITION BY move_type, qty_max_4_tier) AS move_type_qty_max_4_tier_sum_max_qty,
        MAX(min_qty_max_4) OVER (PARTITION BY move_type ORDER BY max_qty DESC, move_type, qty_max_4_tier ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS min_qty_max_4_max_2_prior,
        FIRST_VALUE(max_qty) OVER (PARTITION BY move_type ORDER BY max_qty DESC, move_type, qty_max_4_tier ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_max_qty
    FROM rollup_base
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM sequenced_2
    WHERE max_qty > (SELECT MIN(max_qty) FROM sequenced_2)
)
SELECT
    move_type,
    qty_max_4_tier,
    min_qty_max_4,
    move_type_list,
    move_type_qty_max_4_tier_sum_max_qty,
    min_qty_max_4_max_2_prior,
    first_max_qty,
    CONCAT(qty_max_4_tier, '#', CAST(first_max_qty AS STRING)) AS qty_max_4_tier_ref,
    ABS(min_qty_max_4 - 900) AS min_qty_max_4_distance
FROM filtered
WHERE first_max_qty < 400
ORDER BY qty_max_4_tier_ref NULLS LAST, move_type_qty_max_4_tier_sum_max_qty DESC
