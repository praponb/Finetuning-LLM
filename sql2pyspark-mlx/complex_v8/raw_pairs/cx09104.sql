SELECT id, team AS label
FROM adjusters
WHERE level < 3
EXCEPT ALL
SELECT holder_id AS id, status AS label
FROM policies
WHERE holder_id <= 17
  AND product NOT IN ('HOME', 'TRAVEL')
ORDER BY id
