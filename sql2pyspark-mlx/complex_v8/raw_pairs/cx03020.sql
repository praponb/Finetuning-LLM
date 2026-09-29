WITH base_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment
    FROM policyholders
    WHERE LOWER(holder_name) LIKE '%a %'
)
SELECT id, joined_on
FROM base_holders
WHERE joined_on BETWEEN DATE '2024-02-04' AND DATE '2025-10-01'
UNION ALL
SELECT policy_id, claim_date
FROM claims
UNION ALL
SELECT holder_id, end_date
FROM policies
WHERE holder_id <= 10
ORDER BY id DESC
