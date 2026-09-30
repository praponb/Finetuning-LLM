WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        plan,
        country,
        signup_date,
        birth_year,
        DATE_TRUNC('YEAR', signup_date) AS signup_date_year
    FROM listeners
    WHERE country IS NOT NULL
      AND signup_date < DATE '2025-01-29'
)
SELECT birth_year, signup_date
FROM scoped
UNION ALL
SELECT id AS birth_year, release_date AS signup_date
FROM tracks
WHERE duration_sec <= 352
  AND artist_id IS NOT NULL
