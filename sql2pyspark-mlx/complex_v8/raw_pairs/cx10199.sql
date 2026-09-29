WITH base AS (
    -- Scope the skus rows
    SELECT
        id,
        unit_cost,
        weight_kg,
        labels,
        is_active,
        CASE WHEN is_active THEN 1 ELSE 0 END AS is_active_flag,
        CASE WHEN unit_cost >= 239.3 THEN 'large' WHEN unit_cost >= 108.5 THEN 'medium' ELSE 'small' END AS unit_cost_tier,
        UPPER(TRIM(sku_code)) AS sku_code_clean
    FROM skus
    WHERE UPPER(TRIM(sku_code)) LIKE 'A%'
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ABS(weight_kg - 15.2) AS weight_kg_distance,
        ABS(weight_kg - 14.1) AS weight_kg_distance_2
    FROM base
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(is_active_flag / NULLIF(SUM(is_active_flag) OVER w_unit_cost_tier, 0), 4) AS is_active_flag_share_of_unit_cost_tier,
        ROUND(is_active_flag - AVG(is_active_flag) OVER (PARTITION BY unit_cost_tier), 2) AS is_active_flag_vs_unit_cost_tier_avg,
        MAX(weight_kg_distance) OVER w_unit_cost_tier AS unit_cost_tier_max_weight_kg_distance
    FROM tagged
    WINDOW w_unit_cost_tier AS (PARTITION BY unit_cost_tier)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN unit_cost_tier IN ('small', 'medium') THEN 'group_a' WHEN unit_cost_tier = 'large' THEN 'group_b' ELSE 'other' END AS unit_cost_tier_group,
        COALESCE(unit_cost_tier, 'UNKNOWN') AS unit_cost_tier_or_unknown,
        FLOOR(weight_kg / 5) * 5 AS weight_kg_bucket
    FROM ranked
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, reading
FROM (
    SELECT id, is_active_flag, weight_kg_bucket
    FROM annotated
) src
UNPIVOT (
    reading FOR measure IN (is_active_flag, weight_kg_bucket)
) unp
ORDER BY id, measure
