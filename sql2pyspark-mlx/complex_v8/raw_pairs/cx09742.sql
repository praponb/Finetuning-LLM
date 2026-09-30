SELECT id
FROM policies
EXCEPT
SELECT level AS id
FROM adjusters
