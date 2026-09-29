WITH scoped_listeners AS (
    -- Filter listeners
    SELECT *
    FROM listeners
    WHERE country IN ('KR', 'TH')
      AND LENGTH(TRIM(username)) >= 5
)
SELECT birth_year, id AS ref_id
FROM scoped_listeners
WHERE signup_date >= DATE '2024-10-17'
  AND country = 'UK'
UNION ALL
SELECT id AS birth_year, birth_year AS ref_id
FROM listeners
WHERE plan IN ('student', 'family', 'free')
UNION ALL
SELECT birth_year, id AS ref_id
FROM listeners
ORDER BY birth_year, ref_id
