WITH scoped_listeners AS (
    -- Filter listeners
    SELECT
        id,
        username,
        country,
        birth_year
    FROM listeners
    WHERE (birth_year < 1995 OR signup_date >= DATE '2024-08-02')
),
unioned AS (
    -- Stack the branches
    SELECT id AS qty, birth_year
    FROM scoped_listeners
    UNION ALL
    SELECT birth_year AS qty, id AS birth_year
    FROM listeners
    WHERE signup_date BETWEEN DATE '2024-07-13' AND DATE '2025-03-18'
      AND LOWER(username) LIKE '%ch%'
    UNION ALL
    SELECT track_id AS qty, ms_played AS birth_year
    FROM plays
    WHERE ms_played > 71476
      AND listener_id >= 16
)
SELECT qty, COUNT(*) AS n_rows, SUM(birth_year) AS total_birth_year
FROM unioned
GROUP BY qty
ORDER BY qty
