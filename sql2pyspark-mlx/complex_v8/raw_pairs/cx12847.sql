SELECT id
FROM listeners
EXCEPT ALL
SELECT birth_year AS id
FROM listeners
ORDER BY id
