WITH cleaned_moves AS (
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
        DATEDIFF(DATE '2025-06-30', moved_at) AS days_since_moved_at
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
qualified AS (
    -- Rows that have matching stock_moves
    SELECT s.*
    FROM cleaned_moves s
    WHERE EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.sku_id
              AND st.unit_price >= 111.7
        )
      AND s.qty >= 191
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        moved_at,
        CONCAT_WS(': ', operator, move_type) AS operator_move_type_label
    FROM qualified
),
labeled_2 AS (
    -- Derive helper columns
    SELECT
        id,
        sku_id,
        qty,
        operator_move_type_label,
        DATE_TRUNC('WEEK', moved_at) AS moved_at_week,
        LENGTH(TRIM(operator_move_type_label)) AS operator_move_type_label_len,
        LOWER(REGEXP_REPLACE(TRIM(operator_move_type_label), ' ', '_')) AS operator_move_type_label_slug
    FROM labeled
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(moved_at_week) AS moved_at_week_year,
        ROUND(SUM(operator_move_type_label_len) / NULLIF(SUM(qty), 0), 3) AS operator_move_type_label_len_to_qty_ratio,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(moved_at_week) AS last_moved_at_week
    FROM labeled_2
    GROUP BY YEAR(moved_at_week)
)
-- Final projection
SELECT
    moved_at_week_year,
    operator_move_type_label_len_to_qty_ratio,
    distinct_ids,
    last_moved_at_week
FROM stats
ORDER BY operator_move_type_label_len_to_qty_ratio DESC, moved_at_week_year
LIMIT 10
