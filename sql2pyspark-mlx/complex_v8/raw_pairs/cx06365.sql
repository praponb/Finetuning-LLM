SELECT level, id AS ref_id
FROM adjusters
WHERE level BETWEEN 1 AND 3
UNION ALL
SELECT adjuster_id AS level, id AS ref_id
FROM claims
WHERE reported_at < TIMESTAMP '2025-02-18 13:00:00'
ORDER BY ref_id, level
