WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        CONCAT_WS('|', move_type, operator) AS move_type_operator_label,
        ABS(unit_price - 298.9) AS unit_price_distance
    FROM stock_moves
    WHERE warehouse_id <= 2
),
enriched AS (
    -- Join skus
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.qty,
        s.operator,
        s.move_type_operator_label,
        s.unit_price_distance,
        sk.category AS sku_category,
        sk.labels AS sku_labels,
        sk.unit_cost AS sku_unit_cost
    FROM cleaned_moves s
    LEFT JOIN skus sk ON sk.id = s.sku_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER w_warehouse_id_operator AS rows_in_warehouse_id_operator,
        sku_unit_cost - LAG(sku_unit_cost) OVER (ORDER BY sku_unit_cost, id) AS sku_unit_cost_change,
        SIZE(COLLECT_SET(sku_category) OVER w_warehouse_id_operator) AS distinct_sku_category_in_warehouse_id_operator,
        LAG(sku_unit_cost) OVER (PARTITION BY operator, sku_id ORDER BY unit_price_distance NULLS LAST, id) AS prev_sku_unit_cost,
        MIN(unit_price_distance) OVER (PARTITION BY warehouse_id, operator) AS warehouse_id_operator_min_unit_price_distance,
        NTH_VALUE(unit_price_distance, 2) OVER (PARTITION BY operator, sku_id ORDER BY unit_price_distance NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_price_distance,
        FIRST_VALUE(unit_price_distance) OVER (PARTITION BY sku_category, sku_id ORDER BY unit_price_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_unit_price_distance,
        LAG(qty, 3, 0) OVER (PARTITION BY sku_category, sku_id ORDER BY unit_price_distance ASC, id) AS prev3_qty,
        ROW_NUMBER() OVER w_all_by_sku_unit_cost AS rn,
        MAX(unit_price_distance) OVER (ORDER BY sku_unit_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_unit_price_distance_so_far
    FROM enriched
    WINDOW w_warehouse_id_operator AS (PARTITION BY warehouse_id, operator), w_all_by_sku_unit_cost AS (ORDER BY sku_unit_cost, id)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        operator,
        move_type_operator_label,
        unit_price_distance,
        sku_category,
        sku_labels,
        sku_unit_cost,
        rows_in_warehouse_id_operator,
        distinct_sku_category_in_warehouse_id_operator,
        prev_sku_unit_cost,
        second_unit_price_distance,
        first_unit_price_distance,
        prev3_qty,
        rn,
        ROW_NUMBER() OVER w_all_by_max_unit_price_distan AS rn_2,
        LAG(sku_unit_cost_change) OVER (ORDER BY max_unit_price_distance_so_far, id) AS prev_sku_unit_cost_change,
        ROUND(first_unit_price_distance / NULLIF(SUM(first_unit_price_distance) OVER w_distinct_sku_category_in_war, 0), 4) AS first_unit_price_distance_share_of_distinct_sku_,
        LAST_VALUE(prev_sku_unit_cost) IGNORE NULLS OVER (ORDER BY max_unit_price_distance_so_far, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_sku_unit_cost,
        ROUND(AVG(sku_unit_cost_change) OVER (ORDER BY max_unit_price_distance_so_far, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS sku_unit_cost_change_avg_2_prior,
        SIZE(COLLECT_SET(operator) OVER (PARTITION BY distinct_sku_category_in_warehouse_id_operator, sku_category)) AS distinct_operator_in_distinct_sku_category_in_wa,
        COUNT(*) OVER w_distinct_sku_category_in_war AS rows_in_distinct_sku_category_in_warehouse_id_op
    FROM metrics
    WINDOW w_all_by_max_unit_price_distan AS (ORDER BY max_unit_price_distance_so_far, id), w_distinct_sku_category_in_war AS (PARTITION BY distinct_sku_category_in_warehouse_id_operator, sku_category)
),
totals AS (
    -- Summarise per group
    SELECT
        sku_category,
        rn,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        SUM(distinct_operator_in_distinct_sku_category_in_wa) AS total_distinct_operator_in_distinct_sku_category
    FROM windowed
    WHERE second_unit_price_distance < 300.0
    GROUP BY sku_category, rn
)
-- Final projection
SELECT sku_category, rn
FROM totals
ORDER BY rn NULLS LAST
