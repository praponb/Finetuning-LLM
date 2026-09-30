WITH combined AS (
    -- Stack the branches
    SELECT birth_year AS key_id, signup_date AS day
    FROM listeners
    UNION ALL
    SELECT birth_year AS key_id, signup_date AS day
    FROM listeners
    WHERE birth_year BETWEEN 1967 AND 1988
      AND country IS NOT NULL
)
SELECT key_id, COUNT(*) AS n_rows, MAX(day) AS latest_day
FROM combined
GROUP BY key_id
ORDER BY key_id
