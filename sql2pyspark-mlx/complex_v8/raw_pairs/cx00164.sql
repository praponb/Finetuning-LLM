WITH valid_moves AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        CASE WHEN operator IN ('op_bee', 'op_dew') THEN 'group_a' WHEN operator = 'op_ann' THEN 'group_b' ELSE 'other' END AS operator_group,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM stock_moves
    WHERE (TO_DATE(moved_at) < DATE '2025-03-08' OR warehouse_id >= 2)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        warehouse_id,
        move_type,
        moved_at,
        operator,
        operator_group,
        least_qty_unit_price,
        SUM(qty) OVER (ORDER BY qty DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS qty_sum_3_prior,
        LEAD(qty) OVER (ORDER BY qty DESC, id) AS next_qty,
        ROUND(least_qty_unit_price / NULLIF(SUM(least_qty_unit_price) OVER (PARTITION BY operator, warehouse_id), 0), 4) AS least_qty_unit_price_share_of_operator_warehouse
    FROM valid_moves
)
SELECT id, field, amount
FROM (
    SELECT id, least_qty_unit_price, least_qty_unit_price_share_of_operator_warehouse
    FROM with_windows
) src
UNPIVOT (
    amount FOR field IN (least_qty_unit_price, least_qty_unit_price_share_of_operator_warehouse)
) unp
