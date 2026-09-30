SELECT level
FROM adjusters
EXCEPT ALL
SELECT id
FROM policies
WHERE end_date >= DATE '2026-02-26'
ORDER BY level DESC
