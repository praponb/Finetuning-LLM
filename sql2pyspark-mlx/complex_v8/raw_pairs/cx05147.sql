WITH base_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        MONTH(moved_at) AS moved_at_month,
        COALESCE(move_type, 'UNKNOWN') AS move_type_or_unknown,
        CONCAT_WS(': ', operator, move_type) AS operator_move_type_label
    FROM stock_moves
    WHERE sku_id >= 5
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND(unit_price / NULLIF(moved_at_month, 0), 3) AS unit_price_per_moved_at_month,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label_2,
        DATE_TRUNC('QUARTER', moved_at) AS moved_at_quarter
    FROM base_moves
    WHERE sku_id IS NOT NULL
)
SELECT
    COALESCE(move_type, 'ALL') AS move_type,
    COALESCE(operator, 'ALL') AS operator,
    MIN(unit_price) AS min_unit_price,
    PERCENTILE_APPROX(unit_price_per_moved_at_month, 0.5) AS median_unit_price_per_moved_at_month,
    SUM(CASE WHEN move_type NOT IN ('TRANSFER', 'OUTBOUND') THEN 1 ELSE 0 END) AS n_move_type_not_in_transfer_ou
FROM derived
GROUP BY GROUPING SETS ((move_type, operator), (move_type))
ORDER BY move_type NULLS LAST, operator NULLS LAST
