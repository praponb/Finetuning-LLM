WITH prepared AS (
    -- Scope the policyholders rows
    SELECT
        id,
        region,
        risk_score,
        joined_on,
        SUBSTRING(TRIM(segment), 1, 2) AS segment_prefix
    FROM policyholders
    WHERE LOWER(holder_name) LIKE '%nt%'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        region,
        risk_score,
        FIRST_VALUE(risk_score) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', joined_on) ORDER BY joined_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_risk_score,
        ROUND(AVG(risk_score) OVER (PARTITION BY region), 2) AS region_avg_risk_score,
        MIN(risk_score) OVER (PARTITION BY DATE_TRUNC('MONTH', joined_on) ORDER BY joined_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_risk_score_so_far
    FROM prepared
),
labeled AS (
    -- Add calculated columns
    SELECT *, SUBSTRING(TRIM(region), 1, 3) AS region_prefix
    FROM with_windows
    WHERE first_known_risk_score > 0.1
)
SELECT
    id,
    region,
    risk_score,
    first_known_risk_score,
    region_avg_risk_score,
    min_risk_score_so_far
FROM labeled
ORDER BY min_risk_score_so_far, region DESC
