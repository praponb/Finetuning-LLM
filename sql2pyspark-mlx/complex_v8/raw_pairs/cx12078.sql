WITH matched AS (
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
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, ROUND(AVG(ms_played), 2) AS avg_play_ms_played
    FROM plays
    GROUP BY listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.avg_play_ms_played
    FROM matched s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
)
-- Final projection
SELECT
    id,
    listener_id,
    prior_ms_played_3d,
    avg_play_ms_played,
    ROUND(avg_play_ms_played * 1.5, 2) AS avg_play_ms_played_adj
FROM profiled
ORDER BY listener_id, id NULLS LAST
