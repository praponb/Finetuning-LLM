SELECT holder_id AS ref_id
FROM policies
WHERE product IN ('HEALTH', 'LIFE')
EXCEPT
SELECT id
FROM adjusters
