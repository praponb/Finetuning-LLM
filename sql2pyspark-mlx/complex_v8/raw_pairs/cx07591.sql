SELECT title AS name
FROM tracks
INTERSECT
SELECT plan
FROM listeners
WHERE SIZE(interests) >= 1
  AND plan IN ('student', 'family', 'premium')
