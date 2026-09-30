SELECT id AS key_id
FROM adjusters
EXCEPT
SELECT id AS key_id
FROM adjusters
WHERE team = 'Travel'
