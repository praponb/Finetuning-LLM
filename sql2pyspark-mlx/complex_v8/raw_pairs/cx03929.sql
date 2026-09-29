WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        operator,
        unit_price,
        TO_DATE(moved_at) AS moved_at_date
    FROM stock_moves
    WHERE unit_price BETWEEN 111.7 AND 363.6
)
SELECT sku_id
FROM recent
WHERE move_type IS NOT NULL
  AND warehouse_id IS NOT NULL
EXCEPT
SELECT id
FROM skus
WHERE ARRAY_CONTAINS(labels, 'express')
  AND LENGTH(TRIM(sku_code)) >= 7
ORDER BY sku_id
