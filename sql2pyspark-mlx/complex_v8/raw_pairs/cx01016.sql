WITH base AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        risk_score,
        joined_on,
        segment,
        DATEDIFF(DATE '2025-12-31', joined_on) AS days_since_joined_on
    FROM policyholders
    WHERE segment <> 'VIP'
)
SELECT
    segment,
    joined_on,
    holder_name AS label,
    'holder' AS src
FROM base
UNION
SELECT
    region,
    joined_on,
    segment,
    'holder_2' AS src
FROM policyholders
WHERE segment IS NOT NULL
ORDER BY src DESC, label
