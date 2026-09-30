WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        TO_DATE(moved_at) AS moved_at_date,
        DATE_FORMAT(moved_at, 'yyyy-MM-dd HH:00') AS moved_at_label,
        LEAST(unit_price, qty) AS least_unit_price_qty
    FROM stock_moves
    WHERE HOUR(moved_at) BETWEEN 10 AND 16
      AND warehouse_id IS NOT NULL
),
grouped AS (
    -- Group-level totals
    SELECT
        move_type,
        SUM(CASE WHEN operator = 'op_chan' THEN 1 ELSE 0 END) AS n_operator_op_chan,
        COUNT(*) AS row_count,
        SUM(CASE WHEN operator = 'op_eve' THEN 1 ELSE 0 END) AS n_operator_op_eve,
        COUNT(DISTINCT operator) AS distinct_operators,
        ROUND(SUM(unit_price), 2) AS total_unit_price
    FROM prepared
    WHERE warehouse_id IS NOT NULL
    GROUP BY move_type
    HAVING COUNT(*) >= 2
),
scored AS (
    -- Add calculated columns
    SELECT *, LOWER(REGEXP_REPLACE(TRIM(move_type), ' ', '_')) AS move_type_slug
    FROM grouped
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE total_unit_price > (SELECT AVG(total_unit_price) FROM scored)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        move_type,
        n_operator_op_chan,
        row_count,
        n_operator_op_eve,
        distinct_operators,
        total_unit_price,
        move_type_slug,
        FIRST_VALUE(distinct_operators) OVER (PARTITION BY distinct_operators, n_operator_op_eve ORDER BY total_unit_price DESC, move_type ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_distinct_operators,
        LEAD(distinct_operators, 2) OVER (PARTITION BY move_type ORDER BY row_count DESC NULLS FIRST, move_type) AS next2_distinct_operators
    FROM with_activity
)
-- Final projection
SELECT
    move_type,
    n_operator_op_chan,
    row_count,
    n_operator_op_eve,
    move_type_slug
FROM with_windows
WHERE move_type <> 'TRANSFER'
ORDER BY move_type_slug DESC
