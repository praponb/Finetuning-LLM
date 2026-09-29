WITH base AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE NOT is_active
      AND unit_cost >= 60.9
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        sku_code,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        ROUND(unit_cost / NULLIF(weight_kg, 0), 3) AS unit_cost_per_weight_kg,
        CONCAT(sku_code, '#', CAST(id AS STRING)) AS sku_code_ref,
        ARRAY_JOIN(ARRAY_SORT(labels), ',') AS labels_text
    FROM base
)
SELECT
    id,
    labels,
    unit_cost_per_weight_kg,
    sku_code_ref,
    labels_text,
    LOWER(REGEXP_REPLACE(TRIM(sku_code), ' ', '_')) AS sku_code_slug,
    CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_flag
FROM annotated
WHERE UPPER(TRIM(sku_code)) LIKE 'E%'
ORDER BY id DESC
LIMIT 20
