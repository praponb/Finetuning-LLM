SELECT holder_id AS entity_id, product
FROM policies
WHERE status <> 'LAPSED'
INTERSECT
SELECT level, team
FROM adjusters
WHERE team IN ('Property', 'Motor')
  AND UPPER(TRIM(adjuster_name)) LIKE 'E%'
ORDER BY product, entity_id
