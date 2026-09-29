SELECT plan, signup_date, birth_year
FROM listeners
UNION
SELECT title, release_date, id
FROM tracks
