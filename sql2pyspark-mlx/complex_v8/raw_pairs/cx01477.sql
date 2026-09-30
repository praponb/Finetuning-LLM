SELECT status AS name
FROM claims
WHERE policy_id IS NOT NULL
EXCEPT
SELECT product AS name
FROM policies
WHERE product NOT IN ('AUTO', 'LIFE')
ORDER BY name DESC
