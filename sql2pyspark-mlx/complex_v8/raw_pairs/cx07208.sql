WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        interests,
        YEAR(signup_date) AS signup_date_year
    FROM listeners
    WHERE (NOT ARRAY_CONTAINS(interests, 'live') OR country IN ('JP', 'BR', 'UK'))
)
SELECT id AS key_id, plan AS label
FROM filtered
WHERE plan = 'premium'
EXCEPT
SELECT debut_year, artist_name
FROM artists
WHERE debut_year > 1991
  AND genre = 'Classical'
