WITH scoped_moves AS (
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
        CASE WHEN operator IN ('op_chan', 'op_dew') THEN 'group_a' WHEN operator = 'op_ann' THEN 'group_b' ELSE 'other' END AS operator_group,
        COALESCE(unit_price, 0.0) AS unit_price_filled
    FROM stock_moves
    WHERE operator IN ('op_chan', 'op_ann', 'op_dew')
      AND warehouse_id <= 3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        qty,
        operator,
        unit_price,
        operator_group,
        unit_price_filled,
        LAG(unit_price) OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY moved_at, id) AS prev_unit_price,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY moved_at, id) AS rn,
        ROUND(unit_price_filled / NULLIF(SUM(unit_price_filled) OVER w_sku_id_operator, 0), 4) AS unit_price_filled_share_of_sku_id_operator,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', moved_at) ORDER BY moved_at, id), 3) AS moved_at_cume_dist,
        MAX(qty) OVER w_sku_id_operator AS sku_id_operator_max_qty,
        SIZE(COLLECT_SET(warehouse_id) OVER (PARTITION BY sku_id, operator)) AS distinct_warehouse_id_in_sku_id_operator
    FROM scoped_moves
    WINDOW w_sku_id_operator AS (PARTITION BY sku_id, operator)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY rn DESC) AS rn_dense_rank,
        NTH_VALUE(rn, 2) OVER (PARTITION BY operator_group ORDER BY rn DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_rn,
        ROUND(AVG(rn) OVER (PARTITION BY rn), 2) AS rn_avg_rn,
        ROUND(AVG(unit_price_filled) OVER (ORDER BY rn DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS unit_price_filled_avg_4
    FROM ranked
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(second_rn, 0) AS second_rn_filled,
        CASE WHEN operator IN ('op_chan', 'op_bee') THEN 'group_a' WHEN operator = 'op_dew' THEN 'group_b' ELSE 'other' END AS operator_group_2
    FROM windowed
),
operator_group_stats AS (
    -- Median unit_price_filled_share_of_sku_id_operator per operator_group
    SELECT operator_group, PERCENTILE_APPROX(unit_price_filled_share_of_sku_id_operator, 0.5) AS median_unit_price_filled_share_of_sku_id_operato
    FROM tagged
    GROUP BY operator_group
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_unit_price_filled_share_of_sku_id_operato,
        ROUND(s.unit_price_filled_share_of_sku_id_operator - g.median_unit_price_filled_share_of_sku_id_operato, 2) AS unit_price_filled_share_of_sku_id_operator_minus
    FROM tagged s
    JOIN operator_group_stats g ON g.operator_group = s.operator_group
    WHERE s.unit_price_filled_share_of_sku_id_operator >= g.median_unit_price_filled_share_of_sku_id_operato
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_warehouse_id_in_sku_id_operator,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list,
        COUNT(*) AS n_rows,
        ROUND(STDDEV(sku_id_operator_max_qty), 2) AS stddev_sku_id_operator_max_qty,
        MIN(prev_unit_price) AS min_prev_unit_price
    FROM compared
    WHERE rn_avg_rn >= 3.2
    GROUP BY distinct_warehouse_id_in_sku_id_operator
    HAVING COUNT(*) >= 2
)
SELECT distinct_warehouse_id_in_sku_id_operator, field, amount
FROM (
    SELECT distinct_warehouse_id_in_sku_id_operator, stddev_sku_id_operator_max_qty, min_prev_unit_price
    FROM totals
) src
UNPIVOT (
    amount FOR field IN (stddev_sku_id_operator_max_qty, min_prev_unit_price)
) unp
ORDER BY distinct_warehouse_id_in_sku_id_operator, field
