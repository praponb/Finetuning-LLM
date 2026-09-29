WITH prepared AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        is_active
    FROM skus
    WHERE SIZE(labels) >= 1
),
category_stats AS (
    -- Max unit_cost per category
    SELECT category, MAX(unit_cost) AS max_unit_cost
    FROM prepared
    GROUP BY category
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_unit_cost,
        ROUND(s.unit_cost - g.max_unit_cost, 2) AS unit_cost_minus_max
    FROM prepared s
    JOIN category_stats g ON g.category = s.category
    WHERE s.unit_cost = g.max_unit_cost
),
summary AS (
    -- Group-level totals
    SELECT
        category,
        ARRAY_SORT(COLLECT_SET(category)) AS category_list,
        SUM(CASE WHEN is_active = TRUE THEN unit_cost ELSE 0 END) AS unit_cost_is_active_true,
        PERCENTILE_APPROX(unit_cost, 0.5) AS median_unit_cost
    FROM vs_group
    WHERE unit_cost > 38.3
    GROUP BY category
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        category,
        category_list,
        median_unit_cost,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY median_unit_cost ASC) AS median_unit_cost_dense_rank,
        ROW_NUMBER() OVER w_category_by_median_unit_cost_2 AS median_unit_cost_seq,
        RANK() OVER (ORDER BY median_unit_cost DESC NULLS FIRST, category) AS median_unit_cost_rank,
        SUM(median_unit_cost) OVER w_category AS category_sum_median_unit_cost,
        COUNT(*) OVER (PARTITION BY category) AS rows_in_category,
        MAX(median_unit_cost) OVER (ORDER BY median_unit_cost DESC NULLS FIRST, category ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS median_unit_cost_max_2
    FROM summary
    WINDOW w_category_by_median_unit_cost_2 AS (PARTITION BY category ORDER BY median_unit_cost ASC, category), w_category AS (PARTITION BY category)
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN median_unit_cost_max_2 >= 720.4 THEN 'A' WHEN median_unit_cost_max_2 >= 360.7 THEN 'B' ELSE 'C' END AS median_unit_cost_max_2_tier,
        ABS(median_unit_cost_rank - 6) AS median_unit_cost_rank_distance,
        LEAST(rows_in_category, median_unit_cost_seq) AS least_rows_in_category_median_unit_cost_seq
    FROM with_windows
    WHERE category = 'Apparel'
)
SELECT
    category,
    category_list,
    median_unit_cost,
    median_unit_cost_seq,
    median_unit_cost_rank,
    rows_in_category,
    median_unit_cost_max_2,
    median_unit_cost_max_2_tier,
    median_unit_cost_rank_distance,
    least_rows_in_category_median_unit_cost_seq,
    COALESCE(median_unit_cost_max_2, 0.0) AS median_unit_cost_max_2_filled
FROM enriched
WHERE category NOT IN ('Apparel', 'Electronics')
ORDER BY median_unit_cost_max_2_tier DESC, rows_in_category
