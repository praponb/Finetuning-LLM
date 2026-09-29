SELECT status, id AS qty, 'claim' AS source
FROM claims
UNION ALL
SELECT status, id, 'claim_2' AS source
FROM claims
ORDER BY source, qty
