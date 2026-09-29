WITH base AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        INITCAP(TRIM(operator)) AS operator_clean
    FROM stock_moves
    WHERE sku_id >= 13
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(moved_at, 'yyyy-MM') AS moved_at_ym,
        warehouse_id,
        MAX(moved_at) AS last_moved_at,
        ROUND(100 * COUNT(CASE WHEN operator NOT IN ('op_dew', 'op_chan') THEN 1 END) / COUNT(*), 1) AS pct_operator_not_in_op_dew_op_ch,
        SUM(unit_price) FILTER (WHERE move_type IS NOT NULL) AS unit_price_move_type_is_not_null
    FROM base
    WHERE MONTH(moved_at) IN (1, 2)
    GROUP BY DATE_FORMAT(moved_at, 'yyyy-MM'), warehouse_id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(last_moved_at, 'MMM yyyy') AS last_moved_at_label,
        DATEDIFF(DATE '2025-06-30', last_moved_at) AS days_since_last_moved_at,
        LENGTH(TRIM(moved_at_ym)) AS moved_at_ym_len
    FROM per_group
    WHERE HOUR(last_moved_at) BETWEEN 8 AND 13
)
SELECT
    moved_at_ym,
    warehouse_id,
    last_moved_at,
    pct_operator_not_in_op_dew_op_ch,
    unit_price_move_type_is_not_null,
    last_moved_at_label,
    LENGTH(TRIM(last_moved_at_label)) AS last_moved_at_label_len
FROM enriched
ORDER BY pct_operator_not_in_op_dew_op_ch, moved_at_ym
