SELECT id, status AS name
FROM claims
EXCEPT
SELECT id, status AS name
FROM policies
ORDER BY name, id
