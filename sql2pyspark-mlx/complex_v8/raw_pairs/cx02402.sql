SELECT
    'holder' AS source,
    holder_name,
    joined_on,
    region
FROM policyholders
UNION
SELECT
    'claim' AS source,
    claim_type AS holder_name,
    claim_date AS joined_on,
    status AS region
FROM claims
WHERE claim_type IS NOT NULL
