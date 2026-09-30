WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        category,
        weight_kg,
        labels,
        is_active,
        CONCAT(category, '#', CAST(id AS STRING)) AS category_ref,
        UPPER(TRIM(sku_code)) AS sku_code_clean
    FROM skus
    WHERE UPPER(TRIM(sku_code)) LIKE 'T%'
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM scoped
    WHERE weight_kg > (SELECT MIN(weight_kg) FROM scoped)
),
flattened AS (
    -- One row per label
    SELECT
        s.id,
        s.category,
        s.weight_kg,
        s.is_active,
        s.category_ref,
        s.sku_code_clean,
        t.label_pos,
        t.label
    FROM screened s
    LATERAL VIEW POSEXPLODE(s.labels) t AS label_pos, label
)
SELECT
    id,
    weight_kg,
    label,
    ROUND(label_pos / NULLIF(weight_kg, 0), 3) AS label_pos_per_weight_kg,
    UPPER(TRIM(category)) AS category_clean
FROM flattened
ORDER BY category_clean DESC NULLS FIRST, id DESC
