WITH filtered_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        LOWER(REGEXP_REPLACE(TRIM(category), ' ', '_')) AS category_slug,
        SUBSTRING(TRIM(category), 1, 3) AS category_prefix,
        LENGTH(TRIM(sku_code)) AS sku_code_len
    FROM skus
    WHERE SIZE(labels) >= 1
      AND LENGTH(TRIM(sku_code)) >= 4
),
summary AS (
    -- Summarise per group
    SELECT
        sku_code_len,
        is_active,
        category,
        COUNT(*) FILTER (WHERE LOWER(sku_code) LIKE '%el%') AS n_lower_sku_code_like_el,
        ROUND(SUM(weight_kg) / NULLIF(SUM(sku_code_len), 0), 3) AS weight_kg_to_sku_code_len_ratio,
        ROUND(SUM(unit_cost), 2) AS total_unit_cost,
        MIN(weight_kg) AS min_weight_kg,
        SUM(unit_cost) AS total_unit_cost_2
    FROM filtered_skus
    GROUP BY sku_code_len, is_active, category
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        sku_code_len,
        is_active,
        category,
        n_lower_sku_code_like_el,
        weight_kg_to_sku_code_len_ratio,
        total_unit_cost,
        min_weight_kg,
        ROUND(min_weight_kg - AVG(min_weight_kg) OVER w_category_n_lower_sku_code_li, 2) AS min_weight_kg_vs_category_n_lower_sku_code_like_,
        LAG(sku_code_len) OVER (ORDER BY min_weight_kg, sku_code_len, is_active, category) AS prev_sku_code_len,
        ROUND(AVG(total_unit_cost_2) OVER (PARTITION BY category, n_lower_sku_code_like_el), 2) AS category_n_lower_sku_code_like_el_avg_total_unit,
        NTILE(5) OVER (ORDER BY min_weight_kg, sku_code_len, is_active, category) AS min_weight_kg_quintile
    FROM summary
    WINDOW w_category_n_lower_sku_code_li AS (PARTITION BY category, n_lower_sku_code_like_el)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        IF(is_active, 'yes', 'no') AS is_active_label,
        ROUND(total_unit_cost / NULLIF(n_lower_sku_code_like_el, 0), 3) AS total_unit_cost_per_n_lower_sku_code_like_el,
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_flag
    FROM with_trends
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(is_active_flag) OVER w_n_lower_sku_code_like_el, 2) AS n_lower_sku_code_like_el_avg_is_active_flag,
        LAST_VALUE(is_active_flag) IGNORE NULLS OVER (PARTITION BY n_lower_sku_code_like_el, is_active_label ORDER BY total_unit_cost, sku_code_len, is_active, category ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_active_flag,
        MIN(min_weight_kg_quintile) OVER w_n_lower_sku_code_like_el AS n_lower_sku_code_like_el_min_min_weight_kg_quint
    FROM scored
    WINDOW w_n_lower_sku_code_like_el AS (PARTITION BY n_lower_sku_code_like_el)
),
scored_2 AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN n_lower_sku_code_like_el >= 6 THEN 'A' WHEN n_lower_sku_code_like_el >= 2 THEN 'B' ELSE 'C' END AS n_lower_sku_code_like_el_tier,
        ABS(n_lower_sku_code_like_el - 6) AS n_lower_sku_code_like_el_distance,
        COALESCE(total_unit_cost, 0.0) AS total_unit_cost_filled
    FROM metrics
    WHERE is_active_label IS NOT NULL
)
SELECT
    sku_code_len,
    category,
    weight_kg_to_sku_code_len_ratio,
    total_unit_cost,
    prev_sku_code_len,
    category_n_lower_sku_code_like_el_avg_total_unit,
    min_weight_kg_quintile,
    is_active_label,
    total_unit_cost_per_n_lower_sku_code_like_el,
    n_lower_sku_code_like_el_min_min_weight_kg_quint,
    n_lower_sku_code_like_el_distance,
    total_unit_cost_filled
FROM scored_2
ORDER BY is_active_label, min_weight_kg_quintile
