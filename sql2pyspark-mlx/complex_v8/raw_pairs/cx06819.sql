WITH all_events AS (
    -- Stack the branches
    SELECT
        plan,
        birth_year,
        username,
        'listener' AS origin
    FROM listeners
    UNION ALL
    SELECT
        plan,
        id,
        country,
        'listener_2' AS origin
    FROM listeners
    WHERE SIZE(interests) >= 1
)
SELECT origin, COUNT(*) AS n_rows, SUM(birth_year) AS total_birth_year
FROM all_events
GROUP BY origin
ORDER BY origin
