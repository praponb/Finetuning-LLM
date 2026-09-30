WITH unioned AS (
    -- Stack the branches
    SELECT id AS ref_id, birth_year AS entity_id, signup_date
    FROM listeners
    WHERE SIZE(interests) >= 1
      AND signup_date BETWEEN DATE '2024-07-13' AND DATE '2025-01-29'
    UNION ALL
    SELECT
        artist_id AS ref_id,
        duration_sec AS entity_id,
        release_date AS signup_date
    FROM tracks
    WHERE is_explicit = FALSE
      AND duration_sec > 165
)
SELECT ref_id, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id, MAX(signup_date) AS latest_signup_date
FROM unioned
GROUP BY ref_id
ORDER BY ref_id
