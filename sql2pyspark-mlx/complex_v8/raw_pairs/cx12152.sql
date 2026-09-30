SELECT 'policy' AS src, holder_id AS qty, premium AS metric
FROM policies
WHERE start_date >= DATE '2024-10-22'
  AND premium > 2111.3
UNION ALL
SELECT 'holder' AS src, id, risk_score
FROM policyholders
UNION ALL
SELECT 'claim' AS src, adjuster_id, claimed_amount
FROM claims
WHERE adjuster_id <= 5
