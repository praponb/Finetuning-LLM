WITH filtered AS (
    -- Filter skus
    SELECT
        id,
        category,
        weight_kg,
        labels,
        INITCAP(TRIM(sku_code)) AS sku_code_clean,
        FLOOR(unit_cost / 50) * 50 AS unit_cost_bucket,
        LENGTH(TRIM(category)) AS category_len
    FROM skus
    WHERE NOT is_active
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        labels,
        sku_code_clean,
        unit_cost_bucket,
        category_len,
        GREATEST(weight_kg, unit_cost_bucket) AS greatest_weight_kg_unit_cost_bucket,
        CASE WHEN category IN ('Apparel', 'Electronics') THEN 'group_a' WHEN category = 'Beauty' THEN 'group_b' ELSE 'other' END AS category_group,
        UPPER(TRIM(category)) AS category_clean
    FROM filtered
    WHERE weight_kg BETWEEN 1.8 AND 15.2
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        labels,
        sku_code_clean,
        unit_cost_bucket,
        category_len,
        category_clean,
        ABS(category_len - 6) AS category_len_distance,
        GREATEST(unit_cost_bucket, category_len) AS greatest_unit_cost_bucket_category_len
    FROM tagged
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, greatest_unit_cost_bucket_category_len, category_len
    FROM derived
) src
UNPIVOT (
    reading FOR measure IN (greatest_unit_cost_bucket_category_len, category_len)
) unp
