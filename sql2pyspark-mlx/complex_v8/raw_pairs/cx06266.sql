WITH valid_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment,
        UPPER(TRIM(segment)) AS segment_clean
    FROM policyholders
    WHERE risk_score < 0.6
      AND region = 'West'
),
summary AS (
    -- Summarise per group
    SELECT
        segment,
        region,
        COUNT(*) AS row_count,
        SUM(risk_score) AS total_risk_score,
        ROUND(AVG(risk_score), 2) AS avg_risk_score
    FROM valid_holders
    GROUP BY segment, region
    HAVING COUNT(*) >= 3
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM summary
    WHERE avg_risk_score > (SELECT AVG(avg_risk_score) FROM summary)
)
SELECT *
FROM (
    SELECT segment, region, total_risk_score
    FROM with_activity
) src
PIVOT (
    SUM(total_risk_score)
    FOR region IN ('Central', 'East')
)
ORDER BY segment
