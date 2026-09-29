SELECT id AS ref_id
FROM policies
WHERE premium >= 2013.8
EXCEPT
SELECT id AS ref_id
FROM claims
ORDER BY ref_id
