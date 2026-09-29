SELECT holder_id, id, 'policy' AS source
FROM policies
WHERE status = 'LAPSED'
  AND start_date IS NOT NULL
UNION
SELECT policy_id, adjuster_id, 'claim' AS source
FROM claims
WHERE approved_amount BETWEEN 0.0 AND 38909.4
  AND claim_type = 'fire'
