WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.ms_played) AS prior_ms_played_3d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.listener_id = s.listener_id) AS avg_play_ms_played
    FROM joined_ranges s
)
-- Final projection
SELECT
    id,
    listener_id,
    avg_play_ms_played,
    HOUR(played_at) AS played_at_hour
FROM with_counts
ORDER BY played_at_hour DESC, avg_play_ms_played DESC
