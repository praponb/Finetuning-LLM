SELECT holder_id, status AS label
FROM policies
WHERE holder_id <= 5
INTERSECT
SELECT level AS holder_id, adjuster_name AS label
FROM adjusters
WHERE team <> 'Property'
  AND level < 3
ORDER BY holder_id DESC, label
