SELECT policy_id, reported_at
FROM claims
WHERE adjuster_id <= 6
UNION ALL
SELECT adjuster_id, reported_at
FROM claims
UNION ALL
SELECT id, reported_at
FROM claims
WHERE TO_DATE(reported_at) < DATE '2025-03-02'
  AND status IN ('OPEN', 'IN_REVIEW', 'PAID')
ORDER BY reported_at
