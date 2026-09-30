WITH valid AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels
    FROM skus
    WHERE category IN ('Food', 'Electronics')
      AND weight_kg BETWEEN 6.5 AND 18.5
)
SELECT
    category AS name,
    id,
    weight_kg,
    'sku' AS record_type
FROM valid
WHERE UPPER(TRIM(sku_code)) LIKE 'E%'
  AND SIZE(labels) >= 1
UNION ALL
SELECT
    sku_code,
    id,
    weight_kg,
    'sku_2' AS record_type
FROM skus
