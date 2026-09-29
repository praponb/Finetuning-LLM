WITH unioned AS (
    -- Stack the branches
    SELECT birth_year, signup_date AS event_date, 'listener' AS origin
    FROM listeners
    WHERE birth_year < 1986
      AND plan IN ('premium', 'student', 'free')
    UNION ALL
    SELECT id, signup_date, 'listener_2' AS origin
    FROM listeners
)
SELECT origin, COUNT(*) AS n_rows, SUM(birth_year) AS total_birth_year, MAX(event_date) AS latest_event_date
FROM unioned
GROUP BY origin
ORDER BY origin
