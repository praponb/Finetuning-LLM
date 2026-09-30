SELECT id AS ref_id
FROM artists
EXCEPT ALL
SELECT id AS ref_id
FROM listeners
