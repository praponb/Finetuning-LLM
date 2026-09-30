WITH cleaned_moves AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        moved_at,
        operator,
        unit_price,
        GREATEST(unit_price, qty) AS greatest_unit_price_qty
    FROM stock_moves
    WHERE operator = 'op_dew'
      AND move_type IS NOT NULL
),
filtered AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM cleaned_moves s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.warehouse_id = s.warehouse_id
              AND st.unit_price >= 136.0
        )
),
totals AS (
    -- Group-level totals
    SELECT
        sku_id,
        MAX(unit_price) AS max_unit_price,
        PERCENTILE_APPROX(greatest_unit_price_qty, 0.5) AS median_greatest_unit_price_qty,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        COUNT(DISTINCT move_type) AS distinct_move_types,
        ROUND(SUM(greatest_unit_price_qty), 2) AS total_greatest_unit_price_qty,
        ARRAY_SORT(COLLECT_SET(operator)) AS operator_list
    FROM filtered
    WHERE greatest_unit_price_qty > 125.8
    GROUP BY 1
),
annotated AS (
    -- Label and score rows
    SELECT
        sku_id,
        max_unit_price,
        move_type_list,
        distinct_move_types,
        operator_list,
        SIZE(move_type_list) AS move_type_list_count,
        distinct_move_types - max_unit_price AS distinct_move_types_minus_max_unit_price
    FROM totals
)
-- Final projection
SELECT sku_id, move_type_list, distinct_move_types
FROM annotated
ORDER BY sku_id NULLS LAST, distinct_move_types
LIMIT 5
