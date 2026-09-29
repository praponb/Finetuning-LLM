WITH scoped AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        unit_cost,
        is_active,
        GREATEST(weight_kg, unit_cost) AS greatest_weight_kg_unit_cost,
        IF(is_active, 'yes', 'no') AS is_active_label,
        LENGTH(TRIM(category)) AS category_len
    FROM skus
    WHERE NOT ARRAY_CONTAINS(labels, 'bulky')
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        sku_code,
        is_active,
        greatest_weight_kg_unit_cost,
        is_active_label,
        category_len,
        CONCAT_WS(': ', sku_code, is_active_label) AS sku_code_is_active_label_label,
        LENGTH(TRIM(is_active_label)) AS is_active_label_len,
        CASE WHEN category_len >= 10 THEN 'HIGH' WHEN category_len >= 7 THEN 'MEDIUM' ELSE 'LOW' END AS category_len_tier
    FROM scoped
    WHERE unit_cost > 68.0
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        is_active,
        greatest_weight_kg_unit_cost,
        is_active_label,
        category_len,
        sku_code_is_active_label_label,
        is_active_label_len,
        is_active_label_len - LAG(is_active_label_len) OVER (PARTITION BY is_active_label ORDER BY category_len ASC, id) AS is_active_label_len_change,
        ROUND(greatest_weight_kg_unit_cost - AVG(greatest_weight_kg_unit_cost) OVER (PARTITION BY category_len), 2) AS greatest_weight_kg_unit_cost_vs_category_len_avg,
        ROUND(AVG(greatest_weight_kg_unit_cost) OVER w_category_len, 2) AS category_len_avg_greatest_weight_kg_unit_cost
    FROM derived
    WINDOW w_category_len AS (PARTITION BY category_len)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, category_len, is_active_label_len, is_active_label_len_change
    FROM sequenced
) src
UNPIVOT (
    amount FOR field IN (category_len, is_active_label_len, is_active_label_len_change)
) unp
ORDER BY id, field
