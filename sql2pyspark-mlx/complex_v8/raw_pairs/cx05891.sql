WITH prepared AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        CASE WHEN risk_score >= 0.6 THEN 'HIGH' WHEN risk_score >= 0.3 THEN 'MEDIUM' ELSE 'LOW' END AS risk_score_tier
    FROM policyholders
    WHERE segment IN ('VIP', 'RETAIL')
      AND region = 'North'
)
SELECT id
FROM prepared
INTERSECT
SELECT id
FROM policyholders
WHERE region <> 'East'
