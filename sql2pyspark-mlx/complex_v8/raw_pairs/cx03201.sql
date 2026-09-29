SELECT id
FROM listeners
WHERE country <> 'TH'
EXCEPT
SELECT id
FROM tracks
ORDER BY id
