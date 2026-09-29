WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        MONTH(moved_at) AS moved_at_month,
        LEAST(qty, unit_price) AS least_qty_unit_price
    FROM stock_moves
    WHERE move_type <> 'TRANSFER'
      AND operator IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(qty) OVER w_operator AS operator_sum_qty,
        LAG(unit_price) OVER (PARTITION BY sku_id ORDER BY moved_at_month, id) AS prev_unit_price,
        LAG(moved_at_month, 1, 0) OVER (PARTITION BY moved_at_month, MONTH(moved_at) ORDER BY qty DESC, id) AS prev_moved_at_month
    FROM prepared
    WINDOW w_operator AS (PARTITION BY operator)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(operator)) AS operator_len,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week,
        ROUND(least_qty_unit_price / NULLIF(unit_price, 0), 3) AS least_qty_unit_price_per_unit_price
    FROM sequenced
    WHERE sku_id <= 14
)
SELECT
    operator,
    warehouse_id,
    move_type,
    GROUPING_ID() AS gid,
    ROUND(100 * COUNT(CASE WHEN move_type IN ('INBOUND', 'TRANSFER') THEN 1 END) / COUNT(*), 1) AS pct_move_type_in_inbound_transfe
FROM tagged
GROUP BY ROLLUP(operator, warehouse_id, move_type)
ORDER BY operator NULLS LAST, warehouse_id NULLS LAST, move_type NULLS LAST
