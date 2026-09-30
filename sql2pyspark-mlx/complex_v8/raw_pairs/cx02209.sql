WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        labels,
        is_active,
        LOWER(REGEXP_REPLACE(TRIM(category), ' ', '_')) AS category_slug
    FROM skus
    WHERE weight_kg > 12.4
),
flattened AS (
    -- One row per label
    SELECT
        s.id,
        s.sku_code,
        s.category,
        s.weight_kg,
        s.is_active,
        s.category_slug,
        t.label
    FROM base s
    LATERAL VIEW EXPLODE(s.labels) t AS label
),
category_stats AS (
    -- Max weight_kg per category
    SELECT category, MAX(weight_kg) AS max_weight_kg
    FROM flattened
    GROUP BY category
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_weight_kg,
        ROUND(s.weight_kg - g.max_weight_kg, 2) AS weight_kg_minus_max
    FROM flattened s
    JOIN category_stats g ON g.category = s.category
    WHERE s.weight_kg = g.max_weight_kg
),
latest_per_key AS (
    -- Keep the earliest row per category
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        is_active,
        category_slug,
        label,
        max_weight_kg,
        weight_kg_minus_max
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY weight_kg, id, label) AS rn
        FROM above_average
    ) ranked
    WHERE rn = 1
)
SELECT
    is_active,
    label,
    category,
    COUNT(*) AS row_count
FROM latest_per_key
GROUP BY GROUPING SETS ((is_active, label, category), (is_active), (label))
