WITH prepared_skus AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        sku_code,
        category,
        unit_cost,
        labels,
        is_active,
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_flag,
        LOWER(TRIM(sku_code)) AS sku_code_clean,
        SUBSTRING(TRIM(category), 1, 3) AS category_prefix
    FROM skus
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared_skus
    WHERE unit_cost > (SELECT AVG(unit_cost) FROM prepared_skus)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY unit_cost NULLS LAST, id) AS unit_cost_seq,
        SIZE(COLLECT_SET(category) OVER w_is_active_is_active_flag) AS distinct_category_in_is_active_is_active_flag
    FROM screened
    WINDOW w_is_active_is_active_flag AS (PARTITION BY is_active, is_active_flag)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN unit_cost >= 277.0 THEN 'A' WHEN unit_cost >= 158.7 THEN 'B' ELSE 'C' END AS unit_cost_tier,
        COALESCE(unit_cost_seq, 0) AS unit_cost_seq_filled,
        COALESCE(unit_cost_seq, 0) AS unit_cost_seq_filled_2
    FROM with_windows
    WHERE category NOT IN ('Apparel', 'Toys')
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(unit_cost_seq - 4) AS unit_cost_seq_distance,
        ROUND(is_active_flag / NULLIF(unit_cost, 0), 3) AS is_active_flag_per_unit_cost,
        unit_cost_seq_filled_2 - unit_cost_seq AS unit_cost_seq_filled_2_minus_unit_cost_seq
    FROM labeled
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, unit_cost_seq_filled, distinct_category_in_is_active_is_active_flag, unit_cost_seq
    FROM enriched
) src
UNPIVOT (
    metric_value FOR metric IN (unit_cost_seq_filled, distinct_category_in_is_active_is_active_flag, unit_cost_seq)
) unp
ORDER BY id, metric
