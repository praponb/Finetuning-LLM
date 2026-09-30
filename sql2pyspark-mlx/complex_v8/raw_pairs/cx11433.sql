WITH valid_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        interests
    FROM listeners
    WHERE signup_date IS NOT NULL
      AND plan NOT IN ('free', 'family')
)
SELECT username
FROM valid_listeners
WHERE YEAR(signup_date) = 2024
EXCEPT ALL
SELECT plan
FROM listeners
WHERE plan <> 'family'
