SELECT id AS key_id
FROM adjusters
INTERSECT
SELECT holder_id
FROM policies
