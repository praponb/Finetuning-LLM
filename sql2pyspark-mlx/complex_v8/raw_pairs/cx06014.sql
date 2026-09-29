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
        COALESCE(operator, 'UNKNOWN') AS operator_or_unknown,
        qty - unit_price AS qty_minus_unit_price
    FROM stock_moves
    WHERE move_type = 'ADJUST'
      AND warehouse_id <= 2
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_qty_minus_unit_price AS rn,
        FIRST_VALUE(qty_minus_unit_price) OVER (ORDER BY qty_minus_unit_price DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_qty_minus_unit_price,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id, operator)) AS distinct_warehouse_id_in_sku_id_operator,
        LEAD(qty_minus_unit_price, 2, 0) OVER w_warehouse_id_by_moved_at AS next2_qty_minus_unit_price,
        ROUND(AVG(unit_price) OVER w_sku_id_operator, 2) AS sku_id_operator_avg_unit_price,
        LAG(unit_price) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev_unit_price,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY sku_id, operator)) AS distinct_move_type_in_sku_id_operator,
        ROUND(unit_price - AVG(unit_price) OVER w_sku_id_operator, 2) AS unit_price_vs_sku_id_operator_avg,
        LAG(qty_minus_unit_price, 3) OVER (PARTITION BY warehouse_id ORDER BY moved_at, id) AS prev3_qty_minus_unit_price
    FROM base_moves
    WINDOW w_all_by_qty_minus_unit_price AS (ORDER BY qty_minus_unit_price DESC, id), w_warehouse_id_by_moved_at AS (PARTITION BY warehouse_id ORDER BY moved_at, id), w_sku_id_operator AS (PARTITION BY sku_id, operator)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE rn <= 5
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(next2_qty_minus_unit_price) OVER (ORDER BY qty DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS next2_qty_minus_unit_price_sum_2_prior,
        NTILE(10) OVER (PARTITION BY sku_id, distinct_warehouse_id_in_sku_id_operator ORDER BY unit_price_vs_sku_id_operator_avg ASC, id) AS unit_price_vs_sku_id_operator_avg_decile,
        ROUND(unit_price - AVG(unit_price) OVER w_rn, 2) AS unit_price_vs_rn_avg,
        SIZE(COLLECT_SET(move_type) OVER (PARTITION BY rn)) AS distinct_move_type_in_rn,
        ROUND(AVG(qty) OVER (PARTITION BY sku_id, distinct_warehouse_id_in_sku_id_operator ORDER BY unit_price_vs_sku_id_operator_avg ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS qty_avg_3,
        MIN(prev_unit_price) OVER (PARTITION BY rn) AS rn_min_prev_unit_price
    FROM first_rows
    WINDOW w_rn AS (PARTITION BY rn)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_warehouse_id_in_sku_id_operator,
        distinct_move_type_in_sku_id_operator,
        ROUND(SUM(rn_min_prev_unit_price), 2) AS total_rn_min_prev_unit_price,
        COUNT(DISTINCT operator_or_unknown) AS distinct_operator_or_unknowns,
        MAX(unit_price_vs_sku_id_operator_avg_decile) AS max_unit_price_vs_sku_id_operator_avg_decile
    FROM windowed_2
    GROUP BY distinct_warehouse_id_in_sku_id_operator, distinct_move_type_in_sku_id_operator
)
SELECT
    distinct_warehouse_id_in_sku_id_operator,
    distinct_move_type_in_sku_id_operator,
    total_rn_min_prev_unit_price,
    distinct_operator_or_unknowns
FROM stats
ORDER BY distinct_warehouse_id_in_sku_id_operator NULLS LAST, total_rn_min_prev_unit_price DESC, distinct_move_type_in_sku_id_operator
LIMIT 20
