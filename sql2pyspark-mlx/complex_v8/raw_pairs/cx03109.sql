SELECT id AS entity_id
FROM policies
INTERSECT
SELECT id AS entity_id
FROM adjusters
WHERE team = 'Travel'
