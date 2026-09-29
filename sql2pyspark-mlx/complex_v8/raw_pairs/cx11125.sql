WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        weight_kg,
        labels,
        CONCAT(sku_code, '#', CAST(id AS STRING)) AS sku_code_ref,
        ROUND(weight_kg / NULLIF(unit_cost, 0), 3) AS weight_kg_per_unit_cost,
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_flag
    FROM skus
    WHERE LOWER(sku_code) LIKE '%p-%'
      AND category NOT IN ('Beauty', 'Toys')
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        category,
        SUM(CASE WHEN sku_code IS NOT NULL THEN 1 ELSE 0 END) AS n_sku_code_is_not_null,
        ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list
    FROM scoped
    WHERE category = 'Food'
    GROUP BY 1
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(n_sku_code_is_not_null - 4) AS n_sku_code_is_not_null_distance,
        LOWER(REGEXP_REPLACE(TRIM(category), ' ', '_')) AS category_slug,
        LOWER(REGEXP_REPLACE(TRIM(category), ' ', '_')) AS category_slug_2
    FROM totals
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        category,
        n_sku_code_is_not_null,
        sku_code_list,
        category_slug,
        category_slug_2,
        LEAD(n_sku_code_is_not_null, 1, 0) OVER (PARTITION BY category, n_sku_code_is_not_null_distance ORDER BY n_sku_code_is_not_null_distance ASC, category) AS next_n_sku_code_is_not_null,
        ROUND(PERCENT_RANK() OVER (PARTITION BY category, n_sku_code_is_not_null_distance ORDER BY n_sku_code_is_not_null_distance ASC, category), 3) AS n_sku_code_is_not_null_distance_percent_rank,
        ROUND(n_sku_code_is_not_null / NULLIF(SUM(n_sku_code_is_not_null) OVER (PARTITION BY category), 0), 4) AS n_sku_code_is_not_null_share_of_category
    FROM scored
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER w_next_n_sku_code_is_not_null AS rows_in_next_n_sku_code_is_not_null_category,
        LEAD(next_n_sku_code_is_not_null) OVER (PARTITION BY next_n_sku_code_is_not_null, category ORDER BY n_sku_code_is_not_null_distance_percent_rank, category) AS next_next_n_sku_code_is_not_null,
        ROUND(AVG(next_n_sku_code_is_not_null) OVER w_next_n_sku_code_is_not_null, 2) AS next_n_sku_code_is_not_null_category_avg_next_n_,
        ROUND(n_sku_code_is_not_null / NULLIF(SUM(n_sku_code_is_not_null) OVER (PARTITION BY next_n_sku_code_is_not_null, category), 0), 4) AS n_sku_code_is_not_null_share_of_next_n_sku_code_,
        COUNT(*) OVER (PARTITION BY next_n_sku_code_is_not_null, category ORDER BY n_sku_code_is_not_null_distance_percent_rank, category ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM sequenced
    WINDOW w_next_n_sku_code_is_not_null AS (PARTITION BY next_n_sku_code_is_not_null, category)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM metrics
    WHERE next_next_n_sku_code_is_not_null < (SELECT MAX(next_next_n_sku_code_is_not_null) FROM metrics)
)
-- Final projection
SELECT
    category,
    n_sku_code_is_not_null,
    sku_code_list,
    category_slug,
    category_slug_2,
    next_n_sku_code_is_not_null,
    n_sku_code_is_not_null_distance_percent_rank,
    n_sku_code_is_not_null_share_of_next_n_sku_code_,
    LEAST(running_count, n_sku_code_is_not_null) AS least_running_count_n_sku_code_is_not_null,
    FLOOR(n_sku_code_is_not_null / 2) * 2 AS n_sku_code_is_not_null_bucket
FROM with_activity
WHERE next_n_sku_code_is_not_null_category_avg_next_n_ < 6.5
