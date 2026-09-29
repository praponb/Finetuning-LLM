WITH recent AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE weight_kg > 6.5
),
all_events AS (
    -- Stack the branches
    SELECT 'sku' AS record_type, category, id
    FROM recent
    UNION ALL
    SELECT 'sku_2' AS record_type, sku_code, id
    FROM skus
    WHERE UPPER(TRIM(sku_code)) LIKE 'E%'
)
SELECT record_type, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM all_events
GROUP BY record_type
ORDER BY record_type
