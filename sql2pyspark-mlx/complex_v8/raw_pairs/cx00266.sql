WITH cleaned_skus AS (
    -- Filter skus
    SELECT
        id,
        category,
        labels,
        is_active
    FROM skus
    WHERE ARRAY_CONTAINS(labels, 'express')
      AND is_active = TRUE
)
SELECT id
FROM cleaned_skus
EXCEPT
SELECT id
FROM skus
ORDER BY id
