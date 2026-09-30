WITH scoped_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        joined_on
    FROM policyholders
    WHERE (segment <> 'RETAIL' OR risk_score >= 0.3)
)
SELECT id, joined_on, 'holder' AS src
FROM scoped_holders
UNION ALL
SELECT policy_id AS id, claim_date AS joined_on, 'claim' AS src
FROM claims
WHERE claim_date >= DATE '2024-03-29'
UNION ALL
SELECT id, joined_on, 'holder_3' AS src
FROM policyholders
WHERE joined_on >= DATE '2024-05-23'
ORDER BY joined_on DESC, src
