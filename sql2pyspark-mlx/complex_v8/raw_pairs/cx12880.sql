WITH valid AS (
    -- Scope the listeners rows
    SELECT *
    FROM listeners
    WHERE SIZE(interests) >= 2
      AND YEAR(signup_date) = 2025
)
SELECT id
FROM valid
WHERE birth_year BETWEEN 1966 AND 1994
  AND plan IS NOT NULL
INTERSECT ALL
SELECT debut_year AS id
FROM artists
ORDER BY id
