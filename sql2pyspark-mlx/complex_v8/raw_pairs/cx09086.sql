WITH prepared_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        unit_cost - weight_kg AS unit_cost_minus_weight_kg,
        CONCAT_WS(' / ', category, sku_code) AS category_sku_code_label,
        IF(is_active, 'yes', 'no') AS is_active_label
    FROM skus
    WHERE SIZE(labels) >= 1
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared_skus
    WHERE weight_kg > (SELECT AVG(weight_kg) FROM prepared_skus)
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        category,
        is_active_label,
        ROUND(AVG(weight_kg), 2) AS avg_weight_kg,
        ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list,
        SUM(CASE WHEN category = 'Toys' THEN 1 ELSE 0 END) AS n_category_toys
    FROM qualified
    GROUP BY 1, 2
    HAVING AVG(unit_cost_minus_weight_kg) > 85.1
),
scored AS (
    -- Label and score rows
    SELECT
        category,
        is_active_label,
        n_category_toys,
        LENGTH(TRIM(category)) AS category_len,
        n_category_toys + avg_weight_kg AS n_category_toys_plus_avg_weight_kg,
        LENGTH(TRIM(category)) AS category_len_2
    FROM rollup_base
    WHERE is_active_label IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(category_len_2) OVER (PARTITION BY is_active_label, n_category_toys ORDER BY n_category_toys_plus_avg_weight_kg ASC, category, is_active_label ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS category_len_2_max_7,
        ROUND(CUME_DIST() OVER w_is_active_label_n_category_t_2, 3) AS n_category_toys_plus_avg_weight_kg_cume_dist,
        category_len_2 - LAG(category_len_2) OVER (PARTITION BY is_active_label, n_category_toys ORDER BY n_category_toys_plus_avg_weight_kg ASC, category, is_active_label) AS category_len_2_change,
        ROW_NUMBER() OVER (PARTITION BY is_active_label, n_category_toys ORDER BY n_category_toys_plus_avg_weight_kg ASC, category, is_active_label) AS seq_no
    FROM scored
    WINDOW w_is_active_label_n_category_t_2 AS (PARTITION BY is_active_label, n_category_toys ORDER BY n_category_toys_plus_avg_weight_kg ASC, category, is_active_label)
)
SELECT
    category_len,
    seq_no,
    category_len_2,
    COUNT(DISTINCT is_active_label) AS distinct_is_active_labels,
    COUNT(DISTINCT category) AS distinct_categorys
FROM sequenced
GROUP BY ROLLUP(category_len, seq_no, category_len_2)
ORDER BY category_len NULLS LAST, seq_no NULLS LAST, category_len_2 NULLS LAST
