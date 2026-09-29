WITH scoped AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        labels
    FROM skus
    WHERE LOWER(sku_code) LIKE '%p-%'
)
-- Final projection
SELECT
    id,
    sku_code,
    labels,
    ARRAY_JOIN(ARRAY_SORT(labels), ',') AS labels_text,
    SIZE(labels) AS labels_count
FROM scoped
ORDER BY labels_text, id
LIMIT 10
