SELECT plan AS name
FROM listeners
INTERSECT
SELECT device AS name
FROM plays
WHERE source = 'search'
ORDER BY name DESC
