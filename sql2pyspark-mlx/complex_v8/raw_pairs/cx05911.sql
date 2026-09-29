WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        qty,
        operator,
        unit_price,
        CONCAT_WS(' / ', operator, move_type) AS operator_move_type_label
    FROM stock_moves
)
SELECT qty, operator AS category
FROM recent
UNION
SELECT capacity AS qty, wh_code AS category
FROM warehouses
WHERE opened_on IS NOT NULL
  AND city = 'Kuala Lumpur'
ORDER BY category, qty
