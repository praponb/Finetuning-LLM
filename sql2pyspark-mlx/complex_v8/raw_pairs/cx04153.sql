WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week
    FROM stock_moves
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        ROUND(AVG(unit_price) OVER (PARTITION BY warehouse_id ORDER BY qty ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS unit_price_avg_1_prior,
        LEAD(unit_price, 3) OVER (PARTITION BY warehouse_id ORDER BY qty ASC, id) AS next3_unit_price,
        SUM(qty) OVER (PARTITION BY warehouse_id ORDER BY qty ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_qty
    FROM cleaned_moves
),
per_group AS (
    -- Summarise per group
    SELECT
        sku_id,
        operator,
        move_type,
        MAX(next3_unit_price) AS max_next3_unit_price,
        COUNT(*) FILTER (WHERE move_type NOT IN ('ADJUST', 'INBOUND')) AS n_move_type_not_in_adjust_inbo,
        ROUND(100 * COUNT(CASE WHEN operator IN ('op_dew', 'op_eve') THEN 1 END) / COUNT(*), 1) AS pct_operator_in_op_dew_op_eve
    FROM windowed
    WHERE qty > 67
    GROUP BY sku_id, operator, move_type
)
SELECT sku_id, operator, field, amount
FROM (
    SELECT sku_id, operator, pct_operator_in_op_dew_op_eve, max_next3_unit_price
    FROM per_group
) src
UNPIVOT (
    amount FOR field IN (pct_operator_in_op_dew_op_eve, max_next3_unit_price)
) unp
ORDER BY sku_id, operator, field
