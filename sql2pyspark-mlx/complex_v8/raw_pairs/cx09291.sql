WITH scoped AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        moved_at,
        operator,
        unit_price,
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at,
        CONCAT_WS('|', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE warehouse_id <= 5
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.warehouse_id,
        s.sku_id,
        s.moved_at,
        s.operator,
        s.unit_price,
        s.days_since_moved_at,
        w.city AS warehouse_city
    FROM scoped s
    JOIN warehouses w ON w.id = s.warehouse_id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        operator,
        SUM(CASE WHEN operator = 'op_ann' THEN unit_price ELSE 0 END) AS unit_price_operator_op_ann,
        SUM(days_since_moved_at) AS total_days_since_moved_at,
        ARRAY_SORT(COLLECT_SET(warehouse_city)) AS warehouse_city_list,
        COUNT(DISTINCT operator) AS distinct_operators,
        ROUND(SUM(unit_price) / NULLIF(SUM(days_since_moved_at), 0), 3) AS unit_price_to_days_since_moved_at_ratio,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
    FROM joined
    GROUP BY operator
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE total_days_since_moved_at > (SELECT AVG(total_days_since_moved_at) FROM grouped)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(unit_price_operator_op_ann) OVER (PARTITION BY distinct_operators ORDER BY unit_price_operator_op_ann DESC, operator) AS next_unit_price_operator_op_ann,
        SUM(total_days_since_moved_at) OVER (PARTITION BY operator) AS operator_sum_total_days_since_moved_at,
        ROUND(AVG(unit_price_operator_op_ann) OVER (PARTITION BY operator), 2) AS operator_avg_unit_price_operator_op_ann,
        MAX(total_days_since_moved_at) OVER (PARTITION BY operator) AS operator_max_total_days_since_moved_at
    FROM qualified
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(total_days_since_moved_at / NULLIF(SUM(total_days_since_moved_at) OVER w_operator_distinct_operators, 0), 4) AS total_days_since_moved_at_share_of_operator_dist,
        NTH_VALUE(distinct_operators, 2) OVER (PARTITION BY distinct_operators ORDER BY total_days_since_moved_at ASC, operator ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_operators,
        COUNT(*) OVER (PARTITION BY distinct_operators ORDER BY total_days_since_moved_at ASC, operator ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(total_days_since_moved_at) OVER w_operator_distinct_operators_2 AS prev_total_days_since_moved_at,
        LAST_VALUE(distinct_operators) IGNORE NULLS OVER (PARTITION BY distinct_operators ORDER BY total_days_since_moved_at ASC, operator ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_distinct_operators
    FROM with_windows
    WINDOW w_operator_distinct_operators AS (PARTITION BY operator, distinct_operators), w_operator_distinct_operators_2 AS (PARTITION BY operator, distinct_operators ORDER BY operator_sum_total_days_since_moved_at DESC NULLS FIRST, operator)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM with_windows_2
    WHERE operator_sum_total_days_since_moved_at < (SELECT MAX(operator_sum_total_days_since_moved_at) FROM with_windows_2)
)
SELECT
    operator,
    unit_price_operator_op_ann,
    total_days_since_moved_at,
    warehouse_city_list,
    distinct_operators,
    unit_price_to_days_since_moved_at_ratio,
    operator_list,
    operator_avg_unit_price_operator_op_ann,
    total_days_since_moved_at_share_of_operator_dist,
    running_count,
    prev_total_days_since_moved_at,
    last_known_distinct_operators
FROM with_activity
WHERE last_known_distinct_operators >= 2
ORDER BY prev_total_days_since_moved_at DESC NULLS FIRST
