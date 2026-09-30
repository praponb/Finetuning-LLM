WITH cleaned_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        COALESCE(region, 'UNKNOWN') AS region_or_unknown,
        ABS(risk_score - 0.3) AS risk_score_distance
    FROM policyholders
    WHERE YEAR(joined_on) = 2025
),
grouped AS (
    -- Summarise per group
    SELECT
        region_or_unknown,
        COUNT(*) FILTER (WHERE region = 'Central') AS n_region_central,
        ROUND(SUM(risk_score), 2) AS total_risk_score
    FROM cleaned_holders
    WHERE joined_on < DATE '2025-07-03'
    GROUP BY region_or_unknown
)
SELECT *
FROM (
    SELECT n_region_central, region_or_unknown, total_risk_score
    FROM grouped
) src
PIVOT (
    SUM(total_risk_score) AS amount, COUNT(total_risk_score) AS n
    FOR region_or_unknown IN ('West', 'UNKNOWN', 'South')
)
