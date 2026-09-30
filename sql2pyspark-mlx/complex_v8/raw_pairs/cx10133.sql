WITH recent_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        is_active,
        IF(is_active, 'yes', 'no') AS is_active_label,
        ROUND(weight_kg * 1.07, 2) AS weight_kg_adj
    FROM skus
),
grouped AS (
    -- Group-level totals
    SELECT
        is_active,
        is_active_label,
        category,
        SUM(unit_cost) AS total_unit_cost,
        ARRAY_SORT(COLLECT_SET(category)) AS category_list,
        COUNT(DISTINCT is_active_label) AS distinct_is_active_labels,
        COUNT(*) FILTER (WHERE category = 'Apparel') AS n_category_apparel,
        ROUND(100 * COUNT(CASE WHEN LOWER(sku_code) LIKE '%ap%' THEN 1 END) / COUNT(*), 1) AS pct_lower_sku_code_like_ap
    FROM recent_skus
    GROUP BY 1, 2, 3
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(total_unit_cost) OVER (PARTITION BY is_active_label, category ORDER BY total_unit_cost NULLS LAST, is_active, is_active_label, category) AS prev_total_unit_cost,
        LAG(distinct_is_active_labels) OVER w_is_active_label_category_by AS prev_distinct_is_active_labels,
        FIRST_VALUE(distinct_is_active_labels) IGNORE NULLS OVER (PARTITION BY is_active_label, category ORDER BY total_unit_cost NULLS LAST, is_active, is_active_label, category ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_distinct_is_active_labels
    FROM grouped
    WINDOW w_is_active_label_category_by AS (PARTITION BY is_active_label, category ORDER BY total_unit_cost NULLS LAST, is_active, is_active_label, category)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(n_category_apparel, prev_total_unit_cost) AS greatest_n_category_apparel_prev_total_unit_cost,
        LOWER(REGEXP_REPLACE(TRIM(category), ' ', '_')) AS category_slug,
        GREATEST(total_unit_cost, n_category_apparel) AS greatest_total_unit_cost_n_category_apparel
    FROM metrics
    WHERE SIZE(category_list) >= 2
)
SELECT
    is_active,
    is_active_label,
    category,
    total_unit_cost,
    category_list,
    n_category_apparel,
    pct_lower_sku_code_like_ap,
    prev_total_unit_cost,
    prev_distinct_is_active_labels,
    greatest_n_category_apparel_prev_total_unit_cost,
    greatest_total_unit_cost_n_category_apparel,
    GREATEST(prev_total_unit_cost, n_category_apparel) AS greatest_prev_total_unit_cost_n_category_apparel
FROM scored
ORDER BY greatest_total_unit_cost_n_category_apparel DESC, greatest_n_category_apparel_prev_total_unit_cost DESC
