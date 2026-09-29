WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        category,
        labels,
        is_active,
        COALESCE(category, 'UNKNOWN') AS category_or_unknown
    FROM skus
    WHERE LOWER(sku_code) LIKE '%77%'
)
SELECT id
FROM filtered
WHERE NOT is_active
  AND category_or_unknown IN ('Electronics', 'Beauty')
INTERSECT ALL
SELECT id
FROM skus
WHERE NOT is_active
ORDER BY id DESC
