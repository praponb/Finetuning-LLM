WITH recent AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        labels,
        is_active
    FROM skus
)
SELECT category, id AS qty, unit_cost AS amount
FROM recent
WHERE category IS NOT NULL
  AND unit_cost BETWEEN 108.5 AND 330.6
UNION ALL
SELECT move_type AS category, id AS qty, unit_price AS amount
FROM stock_moves
UNION ALL
SELECT category, id AS qty, weight_kg AS amount
FROM skus
WHERE ARRAY_CONTAINS(labels, 'bulky')
  AND category IS NOT NULL
