WITH cleaned AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        labels,
        SUBSTRING(TRIM(category), 1, 3) AS category_prefix
    FROM skus
    WHERE weight_kg < 14.3
)
SELECT id AS entity_id, category_prefix AS label
FROM cleaned
MINUS
SELECT id, operator
FROM stock_moves
ORDER BY entity_id, label
