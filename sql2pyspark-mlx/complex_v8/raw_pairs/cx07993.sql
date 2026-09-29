WITH filtered_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on
    FROM policyholders
    WHERE region <> 'East'
      AND YEAR(joined_on) = 2024
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        LAG(risk_score) OVER (ORDER BY joined_on, id) AS prev_risk_score,
        FIRST_VALUE(risk_score) IGNORE NULLS OVER (ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_risk_score,
        risk_score - LAG(risk_score) OVER (PARTITION BY region ORDER BY joined_on ASC NULLS LAST, id) AS risk_score_change,
        LAST_VALUE(risk_score) IGNORE NULLS OVER (PARTITION BY region ORDER BY joined_on ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_risk_score,
        ROUND(risk_score / NULLIF(SUM(risk_score) OVER (PARTITION BY region), 0), 4) AS risk_score_share_of_region
    FROM filtered_holders
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT joined_on, region, risk_score_share_of_region
    FROM windowed
) src
PIVOT (
    SUM(risk_score_share_of_region)
    FOR region IN ('West', 'South')
)
ORDER BY joined_on
