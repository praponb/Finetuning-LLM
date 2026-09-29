SELECT plan AS name, signup_date AS day
FROM listeners
UNION ALL
SELECT title, release_date
FROM tracks
ORDER BY name, day
