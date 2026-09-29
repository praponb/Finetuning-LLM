WITH cleaned AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        unit_price
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
      AND operator NOT IN ('op_dew', 'op_eve')
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        warehouse_id,
        MIN(qty) AS min_qty,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        SUM(unit_price) AS total_unit_price,
        COUNT(*) AS n_rows
    FROM cleaned
    GROUP BY 1
)
SELECT warehouse_id, measure, reading
FROM (
    SELECT warehouse_id, n_rows, min_qty
    FROM grouped
) src
UNPIVOT (
    reading FOR measure IN (n_rows, min_qty)
) unp
ORDER BY warehouse_id, measure
