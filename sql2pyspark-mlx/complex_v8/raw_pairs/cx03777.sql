SELECT id
FROM claims
EXCEPT ALL
SELECT id
FROM policies
WHERE premium >= 1042.8
ORDER BY id
