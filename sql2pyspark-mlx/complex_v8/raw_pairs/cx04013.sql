WITH base_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        plan,
        signup_date,
        birth_year,
        interests
    FROM listeners
),
unioned AS (
    -- Stack the branches
    SELECT birth_year, signup_date AS event_date, 'listener' AS src
    FROM base_listeners
    WHERE birth_year >= 1970
    UNION ALL
    SELECT artist_id, release_date, 'track' AS src
    FROM tracks
)
SELECT src, COUNT(*) AS n_rows, SUM(birth_year) AS total_birth_year, MAX(event_date) AS latest_event_date
FROM unioned
GROUP BY src
ORDER BY src
