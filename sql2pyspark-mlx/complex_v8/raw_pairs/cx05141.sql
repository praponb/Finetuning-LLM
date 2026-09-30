WITH scoped_listeners AS (
    -- Filter listeners
    SELECT
        id,
        country,
        signup_date,
        birth_year,
        DATE_FORMAT(signup_date, 'MMM yyyy') AS signup_date_label
    FROM listeners
    WHERE signup_date BETWEEN DATE '2024-04-16' AND DATE '2025-01-29'
      AND plan IN ('student', 'premium')
)
SELECT birth_year
FROM scoped_listeners
WHERE country <> 'JP'
EXCEPT
SELECT id
FROM plays
WHERE is_skipped = TRUE
  AND source IN ('search', 'playlist')
ORDER BY birth_year
