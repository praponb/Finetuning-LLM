WITH recent AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment
    FROM policyholders
    WHERE holder_name IS NOT NULL
)
SELECT
    'holder' AS src,
    id,
    region AS name,
    risk_score AS value
FROM policyholders
WHERE YEAR(joined_on) = 2024
UNION ALL
SELECT
    'holder_2' AS src,
    id,
    segment AS name,
    risk_score AS value
FROM policyholders
WHERE region NOT IN ('West', 'South')
