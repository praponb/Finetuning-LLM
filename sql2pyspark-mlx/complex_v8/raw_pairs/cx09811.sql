WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        ARRAY_CONTAINS(interests, 'live') AS has_live
    FROM listeners
    WHERE birth_year >= 1969
)
SELECT id
FROM recent
WHERE country IN ('BR', 'KR', 'UK')
  AND signup_date BETWEEN DATE '2024-03-21' AND DATE '2025-03-18'
EXCEPT
SELECT id
FROM tracks
WHERE artist_id >= 12
ORDER BY id DESC
