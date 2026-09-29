WITH stacked AS (
    -- Stack the branches
    SELECT 'track' AS record_type, bpm, title
    FROM tracks
    WHERE is_explicit = FALSE
    UNION ALL
    SELECT 'listener' AS record_type, id, plan
    FROM listeners
    WHERE signup_date BETWEEN DATE '2024-03-16' AND DATE '2025-07-08'
    UNION ALL
    SELECT 'play' AS record_type, listener_id, source
    FROM plays
)
SELECT record_type, COUNT(*) AS n_rows, SUM(bpm) AS total_bpm
FROM stacked
GROUP BY record_type
ORDER BY record_type
