WITH valid AS (
    -- Scope the skus rows
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE category <> 'Beauty'
)
SELECT id
FROM valid
EXCEPT
SELECT id
FROM skus
ORDER BY id
