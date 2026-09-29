SELECT id, level AS entity_id, 'adjuster' AS record_type
FROM adjusters
WHERE adjuster_name IS NOT NULL
UNION ALL
SELECT policy_id, adjuster_id, 'claim' AS record_type
FROM claims
WHERE claim_type IN ('flood', 'delay')
  AND claim_date IS NOT NULL
ORDER BY id
