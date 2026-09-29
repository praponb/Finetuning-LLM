WITH recent_skus AS (
    -- Scope the skus rows
    SELECT
        id,
        sku_code,
        unit_cost,
        labels,
        is_active,
        UPPER(TRIM(sku_code)) AS sku_code_clean,
        ROUND(weight_kg / NULLIF(unit_cost, 0), 3) AS weight_kg_per_unit_cost
    FROM skus
    WHERE category = 'Electronics'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(unit_cost, 2, 0) OVER (PARTITION BY is_active ORDER BY weight_kg_per_unit_cost, id) AS next2_unit_cost,
        ROW_NUMBER() OVER (PARTITION BY is_active ORDER BY weight_kg_per_unit_cost, id) AS rn,
        LAG(unit_cost, 1, 0) OVER w_is_active_by_weight_kg_per_u AS prev_unit_cost,
        ROUND(weight_kg_per_unit_cost / NULLIF(SUM(weight_kg_per_unit_cost) OVER (PARTITION BY is_active), 0), 4) AS weight_kg_per_unit_cost_share_of_is_active
    FROM recent_skus
    WINDOW w_is_active_by_weight_kg_per_u AS (PARTITION BY is_active ORDER BY weight_kg_per_unit_cost, id)
),
annotated AS (
    -- Add calculated columns
    SELECT *, SUBSTRING(TRIM(sku_code), 1, 2) AS sku_code_prefix
    FROM windowed
    WHERE ARRAY_CONTAINS(labels, 'cold')
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        is_active,
        rn,
        SUM(next2_unit_cost) FILTER (WHERE is_active = TRUE) AS next2_unit_cost_is_active_true,
        MIN(weight_kg_per_unit_cost) AS min_weight_kg_per_unit_cost,
        ARRAY_SORT(COLLECT_SET(sku_code)) AS sku_code_list,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids
    FROM annotated
    GROUP BY is_active, rn
    HAVING MAX(unit_cost) > 38.3
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE min_weight_kg_per_unit_cost > (SELECT AVG(min_weight_kg_per_unit_cost) FROM stats)
),
tagged AS (
    -- Label and score rows
    SELECT
        is_active,
        rn,
        next2_unit_cost_is_active_true,
        min_weight_kg_per_unit_cost,
        sku_code_list,
        cnt,
        distinct_ids,
        IF(is_active, 'yes', 'no') AS is_active_label,
        ROUND(rn / NULLIF(cnt, 0), 3) AS rn_per_cnt
    FROM screened
),
flattened AS (
    -- One row per sku_code_list_item
    SELECT
        s.is_active,
        s.rn,
        s.next2_unit_cost_is_active_true,
        s.min_weight_kg_per_unit_cost,
        s.cnt,
        s.distinct_ids,
        s.is_active_label,
        s.rn_per_cnt,
        t.sku_code_list_item
    FROM tagged s
    LATERAL VIEW OUTER EXPLODE(s.sku_code_list) t AS sku_code_list_item
)
-- Final projection
SELECT
    is_active,
    next2_unit_cost_is_active_true,
    min_weight_kg_per_unit_cost,
    distinct_ids,
    is_active_label,
    rn_per_cnt,
    LEAST(cnt, rn_per_cnt) AS least_cnt_rn_per_cnt
FROM flattened
ORDER BY least_cnt_rn_per_cnt DESC, is_active_label NULLS LAST
