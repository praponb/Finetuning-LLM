SELECT source, device AS name
FROM plays
UNION
SELECT plan, username
FROM listeners
ORDER BY source DESC, name
