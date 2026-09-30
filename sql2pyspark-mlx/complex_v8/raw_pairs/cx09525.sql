SELECT
    id AS key_id,
    risk_score,
    joined_on,
    'holder' AS src
FROM policyholders
UNION ALL
SELECT
    id,
    risk_score,
    joined_on,
    'holder_2' AS src
FROM policyholders
WHERE region IN ('East', 'West', 'Central')
  AND risk_score < 0.8
