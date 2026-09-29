WITH recent AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        is_active,
        CONCAT(category, '#', CAST(id AS STRING)) AS category_ref
    FROM skus
    WHERE unit_cost > 134.3
      AND category = 'Food'
)
SELECT id
FROM recent
WHERE UPPER(TRIM(sku_code)) LIKE 'E%'
EXCEPT ALL
SELECT sku_id
FROM stock_moves
WHERE operator = 'op_dew'
ORDER BY id DESC
