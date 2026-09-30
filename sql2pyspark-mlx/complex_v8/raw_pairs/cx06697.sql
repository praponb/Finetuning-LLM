WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM listeners
    WHERE signup_date < DATE '2025-01-29'
),
all_events AS (
    -- Stack the branches
    SELECT id, birth_year AS ref_id
    FROM recent
    WHERE ARRAY_CONTAINS(interests, 'live')
    UNION ALL
    SELECT duration_sec, id
    FROM tracks
    UNION ALL
    SELECT ms_played, track_id
    FROM plays
)
SELECT id, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM all_events
GROUP BY id
ORDER BY id
