WITH scoped_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year
    FROM listeners
    WHERE signup_date BETWEEN DATE '2024-07-13' AND DATE '2025-09-26'
),
unioned AS (
    -- Stack the branches
    SELECT id AS entity_id, plan AS category
    FROM scoped_listeners
    UNION ALL
    SELECT id AS entity_id, title AS category
    FROM tracks
    WHERE duration_sec < 309
      AND LENGTH(TRIM(title)) >= 5
)
SELECT category, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id
FROM unioned
GROUP BY category
ORDER BY category
