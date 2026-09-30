WITH filtered_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        CONCAT(segment, '#', CAST(id AS STRING)) AS segment_ref
    FROM policyholders
    WHERE risk_score < 0.7
),
latest_per_key AS (
    -- Keep the latest row per region, segment
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        segment_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY region, segment ORDER BY risk_score DESC, id DESC) AS rn
        FROM filtered_holders
    ) ranked
    WHERE rn = 1
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT joined_on, region, risk_score
    FROM latest_per_key
) src
PIVOT (
    AVG(risk_score)
    FOR region IN ('South', 'East', 'North', 'West')
)
ORDER BY joined_on
