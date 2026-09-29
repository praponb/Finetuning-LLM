WITH prepared AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        labels
    FROM skus
    WHERE unit_cost >= 134.3
      AND sku_code IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(unit_cost) IGNORE NULLS OVER (PARTITION BY category ORDER BY unit_cost ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_cost,
        unit_cost - LAG(unit_cost) OVER w_category_by_unit_cost AS unit_cost_change,
        NTH_VALUE(unit_cost, 2) OVER (PARTITION BY category ORDER BY unit_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_unit_cost,
        DENSE_RANK() OVER w_category_by_unit_cost_ties AS unit_cost_dense_rank
    FROM prepared
    WINDOW w_category_by_unit_cost AS (PARTITION BY category ORDER BY unit_cost DESC, id), w_category_by_unit_cost_ties AS (PARTITION BY category ORDER BY unit_cost DESC)
),
category_stats AS (
    -- Avg last_known_unit_cost per category
    SELECT category, AVG(last_known_unit_cost) AS avg_last_known_unit_cost
    FROM with_windows
    GROUP BY category
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_last_known_unit_cost,
        ROUND(s.last_known_unit_cost - g.avg_last_known_unit_cost, 2) AS last_known_unit_cost_minus_avg
    FROM with_windows s
    JOIN category_stats g ON g.category = s.category
    WHERE s.last_known_unit_cost > g.avg_last_known_unit_cost
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        unit_cost_dense_rank,
        category,
        ROUND(AVG(unit_cost_dense_rank), 2) AS avg_unit_cost_dense_rank,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        SUM(CASE WHEN category = 'Electronics' THEN unit_cost_change ELSE 0 END) AS unit_cost_change_category_electronics,
        ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list
    FROM outliers
    GROUP BY unit_cost_dense_rank, category
    HAVING COUNT(*) >= 2
),
filtered AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE unit_cost_dense_rank < (SELECT MAX(unit_cost_dense_rank) FROM totals)
),
tagged AS (
    -- Label and score rows
    SELECT
        unit_cost_dense_rank,
        category,
        avg_unit_cost_dense_rank,
        distinct_ids,
        unit_cost_change_category_electronics,
        LENGTH(TRIM(category)) AS category_len,
        ARRAY_JOIN(ARRAY_SORT(sku_code_list), ',') AS sku_code_list_text
    FROM filtered
),
scored AS (
    -- Add calculated columns
    SELECT
        unit_cost_dense_rank,
        category,
        avg_unit_cost_dense_rank,
        distinct_ids,
        unit_cost_change_category_electronics,
        sku_code_list_text,
        ROUND(unit_cost_dense_rank / NULLIF(category_len, 0), 3) AS unit_cost_dense_rank_per_category_len
    FROM tagged
)
-- Final projection
SELECT
    unit_cost_dense_rank,
    category,
    avg_unit_cost_dense_rank,
    distinct_ids,
    unit_cost_change_category_electronics,
    unit_cost_dense_rank_per_category_len,
    GREATEST(distinct_ids, unit_cost_dense_rank) AS greatest_distinct_ids_unit_cost_dense_rank,
    SUBSTRING(TRIM(sku_code_list_text), 1, 3) AS sku_code_list_text_prefix
FROM scored
ORDER BY unit_cost_change_category_electronics DESC
