WITH recent_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM stock_moves
    WHERE (operator = 'op_bee' OR unit_price > 236.5)
),
unique_rows AS (
    -- Keep the latest row per move_type, sku_id
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY move_type, sku_id ORDER BY moved_at DESC, id DESC) AS row_num
        FROM recent_moves
    ) ranked
    WHERE row_num = 1
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        move_type,
        qty,
        moved_at,
        operator,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        CONCAT_WS(': ', move_type, operator) AS move_type_operator_label,
        DATE_TRUNC('HOUR', moved_at) AS moved_at_hour
    FROM unique_rows
    WHERE move_type = 'TRANSFER'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        move_type,
        warehouse_id,
        DATE_TRUNC('MONTH', moved_at_hour) AS moved_at_hour_month,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT operator) AS distinct_operators
    FROM annotated
    GROUP BY move_type, warehouse_id, DATE_TRUNC('MONTH', moved_at_hour)
)
SELECT
    move_type,
    warehouse_id,
    moved_at_hour_month,
    n_rows,
    distinct_operators,
    LEAST(n_rows, distinct_operators) AS least_n_rows_distinct_operators,
    DATE_TRUNC('DAY', moved_at_hour_month) AS moved_at_hour_month_day
FROM summary
ORDER BY least_n_rows_distinct_operators DESC, n_rows, move_type, warehouse_id, moved_at_hour_month
LIMIT 20
