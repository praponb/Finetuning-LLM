SELECT id
FROM policyholders
INTERSECT
SELECT id
FROM policies
ORDER BY id DESC
