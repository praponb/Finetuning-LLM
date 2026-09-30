WITH cleaned_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        LOWER(TRIM(segment)) AS segment_clean
    FROM policyholders
    WHERE joined_on >= DATE '2024-10-23'
      AND segment IN ('VIP', 'SME')
),
summary AS (
    -- Group-level totals
    SELECT
        region,
        segment,
        COUNT(*) AS cnt,
        SUM(risk_score) AS total_risk_score,
        COUNT(DISTINCT id) AS distinct_ids
    FROM cleaned_holders
    WHERE segment = 'VIP'
    GROUP BY 1, 2
    HAVING SUM(risk_score) > 0.2
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN segment IN ('VIP', 'SME') THEN 'group_a' WHEN segment = 'RETAIL' THEN 'group_b' ELSE 'other' END AS segment_group,
        ROUND(total_risk_score * 0.9, 2) AS total_risk_score_adj
    FROM summary
    WHERE distinct_ids >= 2
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE distinct_ids > (SELECT AVG(distinct_ids) FROM labeled)
),
qualified_2 AS (
    -- Compare against the overall min
    SELECT *
    FROM qualified
    WHERE total_risk_score_adj > (SELECT MIN(total_risk_score_adj) FROM qualified)
)
-- Final projection
SELECT
    region,
    segment,
    segment_group,
    CONCAT(segment, '#', CAST(cnt AS STRING)) AS segment_ref
FROM qualified_2
ORDER BY segment_ref DESC, region, segment
LIMIT 20
