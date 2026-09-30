SELECT holder_id AS key_id, id, 'policy' AS source
FROM policies
UNION
SELECT id AS key_id, level AS id, 'adjuster' AS source
FROM adjusters
WHERE team = 'Health'
ORDER BY id DESC, key_id
