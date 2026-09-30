WITH unioned AS (
    -- Stack the branches
    SELECT 'listener' AS source, id, signup_date
    FROM listeners
    WHERE signup_date >= DATE '2024-08-02'
    UNION ALL
    SELECT 'track' AS source, duration_sec AS id, release_date AS signup_date
    FROM tracks
    WHERE SIZE(moods) >= 2
      AND title IS NOT NULL
    UNION ALL
    SELECT 'listener_3' AS source, birth_year AS id, signup_date
    FROM listeners
    WHERE SIZE(interests) >= 2
)
SELECT source, COUNT(*) AS n_rows, SUM(id) AS total_id, MAX(signup_date) AS latest_signup_date
FROM unioned
GROUP BY source
ORDER BY source
