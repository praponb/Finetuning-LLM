SELECT policy_id AS qty, adjuster_id AS key_id
FROM claims
WHERE status IS NOT NULL
UNION ALL
SELECT adjuster_id AS qty, id AS key_id
FROM claims
WHERE claim_date BETWEEN DATE '2024-03-30' AND DATE '2025-04-20'
  AND adjuster_id IS NOT NULL
UNION ALL
SELECT id AS qty, holder_id AS key_id
FROM policies
WHERE status = 'ACTIVE'
  AND end_date < DATE '2026-06-18'
ORDER BY qty, key_id
