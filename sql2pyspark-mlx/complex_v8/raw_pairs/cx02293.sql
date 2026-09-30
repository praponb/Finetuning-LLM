SELECT level
FROM adjusters
EXCEPT
SELECT level
FROM adjusters
ORDER BY level
