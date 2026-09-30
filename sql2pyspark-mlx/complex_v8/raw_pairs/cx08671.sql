WITH prepared_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price,
        LOWER(REGEXP_REPLACE(TRIM(operator), ' ', '_')) AS operator_slug,
        TO_DATE(moved_at) AS moved_at_date,
        CASE WHEN operator IN ('op_ann', 'op_eve') THEN 'group_a' WHEN operator = 'op_dew' THEN 'group_b' ELSE 'other' END AS operator_group
    FROM stock_moves
    WHERE qty BETWEEN 99 AND 348
),
scored AS (
    -- Add calculated columns
    SELECT *, DATE_TRUNC('WEEK', moved_at_date) AS moved_at_date_week
    FROM prepared_moves
),
grouped AS (
    -- Summarise per group
    SELECT
        move_type,
        DATE_TRUNC('MONTH', moved_at_date) AS moved_at_date_month,
        COUNT(*) AS cnt,
        COUNT(DISTINCT sku_id) AS distinct_sku_ids,
        SUM(qty) AS total_qty,
        MAX(unit_price) AS max_unit_price,
        SUM(CASE WHEN move_type IN ('OUTBOUND', 'ADJUST', 'TRANSFER') THEN 1 ELSE 0 END) AS n_move_type_in_outbound_adjust,
        COUNT(DISTINCT id) AS distinct_ids
    FROM scored
    GROUP BY move_type, DATE_TRUNC('MONTH', moved_at_date)
    HAVING AVG(unit_price) > 160.8
)
-- Final projection
SELECT
    move_type,
    moved_at_date_month,
    distinct_sku_ids,
    n_move_type_in_outbound_adjust,
    distinct_ids
FROM grouped
ORDER BY moved_at_date_month DESC
