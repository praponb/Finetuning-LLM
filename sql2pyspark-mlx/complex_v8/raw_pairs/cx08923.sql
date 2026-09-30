SELECT
    'claim' AS record_type,
    policy_id AS entity_id,
    adjuster_id,
    reported_at
FROM claims
WHERE policy_id >= 20
  AND status = 'REJECTED'
UNION ALL
SELECT
    'claim_2' AS record_type,
    policy_id,
    adjuster_id,
    reported_at
FROM claims
WHERE status IN ('IN_REVIEW', 'PAID')
  AND claimed_amount > 12726.7
ORDER BY record_type DESC
