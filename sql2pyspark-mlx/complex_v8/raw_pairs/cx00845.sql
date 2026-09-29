WITH prepared_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active
    FROM skus
    WHERE weight_kg > 3.7
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN category IN ('Toys', 'Food') THEN 'group_a' WHEN category = 'Apparel' THEN 'group_b' ELSE 'other' END AS category_group,
        CASE WHEN category IN ('Electronics', 'Beauty') THEN 'group_a' WHEN category = 'Toys' THEN 'group_b' ELSE 'other' END AS category_group_2,
        LOWER(REGEXP_REPLACE(TRIM(category), ' ', '_')) AS category_slug
    FROM prepared_skus
    WHERE weight_kg < 14.6
),
per_label AS (
    -- One row per label
    SELECT
        s.id,
        s.category,
        s.unit_cost,
        s.weight_kg,
        s.is_active,
        s.category_group,
        s.category_group_2,
        s.category_slug,
        t.label
    FROM annotated s
    LATERAL VIEW EXPLODE(s.labels) t AS label
)
-- Subtotals with CUBE
SELECT
    COALESCE(category_group_2, 'ALL') AS category_group_2,
    COALESCE(category, 'ALL') AS category,
    COALESCE(category_group, 'ALL') AS category_group,
    GROUPING(category_group_2) AS is_total_category_group_2,
    ROUND(STDDEV(weight_kg), 2) AS stddev_weight_kg,
    ARRAY_SORT(COLLECT_SET(category)) AS category_list,
    ROUND(AVG(unit_cost), 2) AS avg_unit_cost
FROM per_label
GROUP BY CUBE(category_group_2, category, category_group)
