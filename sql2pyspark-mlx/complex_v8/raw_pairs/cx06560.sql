SELECT id AS ref_id, reported_at, adjuster_id AS qty
FROM claims
UNION ALL
SELECT adjuster_id AS ref_id, reported_at, id AS qty
FROM claims
WHERE approved_amount <= 32510.7
  AND MONTH(reported_at) IN (2, 3)
