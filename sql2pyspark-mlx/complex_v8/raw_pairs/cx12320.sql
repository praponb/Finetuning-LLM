WITH scoped AS (
    -- Filter skus
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        labels,
        SUBSTRING(TRIM(sku_code), 1, 3) AS sku_code_prefix
    FROM skus
    WHERE is_active = FALSE
      AND category <> 'Beauty'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY category) AS rows_in_category,
        LAST_VALUE(unit_cost) OVER (ORDER BY unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_unit_cost,
        LAST_VALUE(unit_cost) IGNORE NULLS OVER (ORDER BY unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_cost,
        ROUND(unit_cost - AVG(unit_cost) OVER (PARTITION BY category), 2) AS unit_cost_vs_category_avg,
        SUM(unit_cost) OVER (ORDER BY unit_cost ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS unit_cost_sum_3,
        unit_cost - LAG(unit_cost) OVER (ORDER BY unit_cost DESC, id) AS unit_cost_change,
        unit_cost - LAG(unit_cost) OVER w_all_by_unit_cost_2 AS unit_cost_change_2,
        MIN(unit_cost) OVER (ORDER BY unit_cost DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_unit_cost_so_far,
        NTH_VALUE(unit_cost, 2) OVER (ORDER BY unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_cost,
        MAX(unit_cost) OVER (PARTITION BY category) AS category_max_unit_cost,
        ROUND(PERCENT_RANK() OVER (ORDER BY unit_cost ASC), 3) AS unit_cost_percent_rank,
        FIRST_VALUE(unit_cost) IGNORE NULLS OVER (ORDER BY unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_cost,
        ROUND(CUME_DIST() OVER w_all_by_unit_cost, 3) AS unit_cost_cume_dist,
        ROUND(AVG(unit_cost) OVER (PARTITION BY category), 2) AS category_avg_unit_cost
    FROM scoped
    WINDOW w_all_by_unit_cost_2 AS (ORDER BY unit_cost ASC, id), w_all_by_unit_cost AS (ORDER BY unit_cost DESC, id)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        labels,
        sku_code_prefix,
        rows_in_category,
        last_unit_cost,
        last_known_unit_cost,
        unit_cost_change,
        unit_cost_change_2,
        min_unit_cost_so_far,
        second_unit_cost,
        category_max_unit_cost,
        first_known_unit_cost,
        category_avg_unit_cost,
        COUNT(*) OVER (PARTITION BY category) AS rows_in_category_2,
        LAST_VALUE(category_max_unit_cost) OVER (PARTITION BY category ORDER BY unit_cost_change_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_category_max_unit_cost,
        SUM(unit_cost_change_2) OVER (PARTITION BY category ORDER BY category_max_unit_cost DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS unit_cost_change_2_sum_2_prior,
        NTILE(10) OVER (PARTITION BY category ORDER BY first_known_unit_cost, id) AS first_known_unit_cost_decile,
        ROUND(PERCENT_RANK() OVER w_category_by_category_max_uni_2, 3) AS category_max_unit_cost_percent_rank,
        RANK() OVER (PARTITION BY category ORDER BY first_known_unit_cost) AS first_known_unit_cost_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY category ORDER BY first_known_unit_cost, id), 3) AS first_known_unit_cost_cume_dist,
        LEAD(unit_cost_change_2) OVER (PARTITION BY category ORDER BY category_max_unit_cost DESC, id) AS next_unit_cost_change_2
    FROM with_windows
    WINDOW w_category_by_category_max_uni_2 AS (PARTITION BY category ORDER BY category_max_unit_cost DESC, id)
),
summary AS (
    -- Summarise per group
    SELECT
        CASE WHEN last_unit_cost >= 277.0 THEN 'large' WHEN last_unit_cost >= 108.5 THEN 'medium' ELSE 'small' END AS last_unit_cost_tier,
        COUNT(*) AS n_rows,
        SUM(CASE WHEN sku_code IS NOT NULL THEN 1 ELSE 0 END) AS n_sku_code_is_not_null,
        ARRAY_SORT(COLLECT_SET(category)) AS category_list
    FROM with_trends
    WHERE last_unit_cost <= 306.4
    GROUP BY CASE WHEN last_unit_cost >= 277.0 THEN 'large' WHEN last_unit_cost >= 108.5 THEN 'medium' ELSE 'small' END
    HAVING COUNT(*) >= 2
)
SELECT
    last_unit_cost_tier,
    n_rows,
    n_rows - n_sku_code_is_not_null AS n_rows_minus_n_sku_code_is_not_null,
    FLOOR(n_sku_code_is_not_null / 2) * 2 AS n_sku_code_is_not_null_bucket
FROM summary
WHERE n_rows < 8
ORDER BY n_sku_code_is_not_null_bucket
