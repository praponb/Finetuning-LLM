WITH valid_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE city IN ('Singapore', 'Bangkok')
)
SELECT wh_code AS category, city AS category_2, 'warehouse' AS record_type
FROM warehouses
UNION
SELECT move_type, operator, 'move' AS record_type
FROM stock_moves
WHERE move_type = 'OUTBOUND'
  AND operator = 'op_dew'
ORDER BY category_2
