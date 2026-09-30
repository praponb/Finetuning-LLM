WITH base_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        weight_kg,
        is_active,
        SUBSTRING(TRIM(category), 1, 2) AS category_prefix,
        INITCAP(TRIM(sku_code)) AS sku_code_clean
    FROM skus
    WHERE weight_kg >= 12.4
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        category,
        weight_kg,
        is_active,
        category_prefix,
        sku_code_clean,
        weight_kg - LAG(weight_kg) OVER (ORDER BY weight_kg, id) AS weight_kg_change,
        COUNT(*) OVER w_category AS rows_in_category,
        FIRST_VALUE(weight_kg) IGNORE NULLS OVER (ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_weight_kg,
        ROUND(AVG(weight_kg) OVER (ORDER BY weight_kg, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS weight_kg_avg_1_prior
    FROM base_skus
    WINDOW w_category AS (PARTITION BY category)
),
with_activity AS (
    -- Rows with no matching stock_moves
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM stock_moves st
            WHERE st.sku_id = s.id
              AND st.warehouse_id IS NOT NULL
              AND st.unit_price BETWEEN 152.7 AND 329.7
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(weight_kg) OVER (PARTITION BY is_active ORDER BY first_known_weight_kg DESC, id) AS prev_weight_kg,
        MAX(weight_kg_avg_1_prior) OVER (PARTITION BY is_active, category) AS is_active_category_max_weight_kg_avg_1_prior,
        NTILE(10) OVER (PARTITION BY is_active ORDER BY first_known_weight_kg DESC, id) AS first_known_weight_kg_decile
    FROM with_activity
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        category,
        is_active,
        category_prefix,
        weight_kg_change,
        prev_weight_kg,
        is_active_category_max_weight_kg_avg_1_prior,
        ROUND(AVG(weight_kg_change) OVER (PARTITION BY is_active ORDER BY weight_kg ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS weight_kg_change_avg_6_prior,
        COUNT(*) OVER (PARTITION BY category, is_active) AS rows_in_category_is_active,
        ROUND(AVG(weight_kg_avg_1_prior) OVER (PARTITION BY category ORDER BY first_known_weight_kg ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS weight_kg_avg_1_prior_avg_4
    FROM windowed
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        category,
        is_active,
        category_prefix,
        weight_kg_change,
        is_active_category_max_weight_kg_avg_1_prior,
        weight_kg_change_avg_6_prior,
        rows_in_category_is_active,
        weight_kg_avg_1_prior_avg_4,
        LENGTH(TRIM(category)) AS category_len,
        LOWER(REGEXP_REPLACE(TRIM(category), ' ', '_')) AS category_slug
    FROM with_windows
)
SELECT
    id,
    is_active,
    category_prefix,
    weight_kg_change,
    weight_kg_change_avg_6_prior,
    rows_in_category_is_active,
    category_len,
    ABS(category_len - 6) AS category_len_distance
FROM derived
WHERE category_prefix IS NOT NULL
ORDER BY category_prefix DESC NULLS FIRST
