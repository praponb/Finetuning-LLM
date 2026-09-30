SELECT id AS key_id
FROM policies
INTERSECT
SELECT holder_id AS key_id
FROM policies
ORDER BY key_id DESC
