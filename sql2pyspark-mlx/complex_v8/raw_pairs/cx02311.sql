SELECT policy_id, id, 'claim' AS source
FROM claims
WHERE reported_at >= TIMESTAMP '2025-01-19 15:00:00'
UNION ALL
SELECT id, holder_id, 'policy' AS source
FROM policies
WHERE product IN ('LIFE', 'HOME')
