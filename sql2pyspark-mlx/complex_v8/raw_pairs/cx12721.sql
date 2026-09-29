SELECT holder_id AS qty
FROM policies
INTERSECT
SELECT holder_id
FROM policies
ORDER BY qty
