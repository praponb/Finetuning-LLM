WITH cleaned_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        operator,
        unit_price,
        CONCAT_WS(' / ', move_type, operator) AS move_type_operator_label
    FROM stock_moves
    WHERE move_type = 'OUTBOUND'
)
SELECT move_type
FROM cleaned_moves
EXCEPT
SELECT operator
FROM stock_moves
WHERE move_type = 'TRANSFER'
  AND qty >= 111
ORDER BY move_type DESC
