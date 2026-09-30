WITH recent_skus AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        CASE WHEN category IN ('Beauty', 'Apparel') THEN 'group_a' WHEN category = 'Food' THEN 'group_b' ELSE 'other' END AS category_group
    FROM skus
    WHERE is_active = FALSE
),
totals AS (
    -- Group-level totals
    SELECT
        category,
        category_group,
        ARRAY_SORT(COLLECT_SET(category)) AS category_list,
        ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list,
        COUNT(*) AS n_rows,
        MIN(weight_kg) AS min_weight_kg
    FROM recent_skus
    GROUP BY category, category_group
    HAVING COUNT(*) >= 2
),
flattened AS (
    -- One row per sku_code_list_item
    SELECT
        s.category,
        s.category_group,
        s.category_list,
        s.n_rows,
        s.min_weight_kg,
        t.sku_code_list_item
    FROM totals s
    LATERAL VIEW EXPLODE(s.sku_code_list) t AS sku_code_list_item
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(min_weight_kg, 2) OVER (PARTITION BY sku_code_list_item, category ORDER BY min_weight_kg DESC NULLS FIRST, category, category_group, sku_code_list_item ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_min_weight_kg,
        MAX(n_rows) OVER (ORDER BY min_weight_kg DESC, category, category_group, sku_code_list_item ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_n_rows_so_far,
        ROUND(n_rows - AVG(n_rows) OVER w_sku_code_list_item_category_2, 2) AS n_rows_vs_sku_code_list_item_category_group_avg,
        DENSE_RANK() OVER (PARTITION BY category_group ORDER BY n_rows DESC, category, category_group, sku_code_list_item) AS n_rows_dense_rank,
        NTH_VALUE(n_rows, 2) OVER (PARTITION BY sku_code_list_item, category ORDER BY min_weight_kg DESC NULLS FIRST, category, category_group, sku_code_list_item ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_rows
    FROM flattened
    WINDOW w_sku_code_list_item_category_2 AS (PARTITION BY sku_code_list_item, category_group)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(sku_code_list_item)) AS sku_code_list_item_len,
        UPPER(TRIM(category_group)) AS category_group_clean,
        CONCAT_WS(': ', category_group, category) AS category_group_category_label
    FROM ranked
)
-- Final projection
SELECT
    category,
    category_list,
    n_rows,
    sku_code_list_item,
    second_min_weight_kg,
    max_n_rows_so_far,
    n_rows_vs_sku_code_list_item_category_group_avg,
    n_rows_dense_rank,
    second_n_rows,
    category_group_clean,
    category_group_category_label
FROM tagged
WHERE n_rows_vs_sku_code_list_item_category_group_avg BETWEEN -10.8 AND 2.4
ORDER BY n_rows_dense_rank DESC
