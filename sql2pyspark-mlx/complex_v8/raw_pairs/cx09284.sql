WITH stacked AS (
    -- Stack the branches
    SELECT 'play' AS src, track_id AS ref_id, played_at
    FROM plays
    UNION ALL
    SELECT 'play_2' AS src, listener_id, played_at
    FROM plays
    WHERE is_skipped = TRUE
      AND device IN ('desktop', 'speaker')
)
SELECT src, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id, MAX(played_at) AS latest_played_at
FROM stacked
GROUP BY src
ORDER BY src
