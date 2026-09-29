WITH scoped_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        opened_on
    FROM warehouses
    WHERE LENGTH(TRIM(wh_code)) >= 7
)
SELECT id
FROM scoped_warehouses
EXCEPT
SELECT sku_id
FROM stock_moves
WHERE operator = 'op_eve'
  AND qty BETWEEN 112 AND 357
