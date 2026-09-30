WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.ms_played) AS prior_ms_played_7d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MIN(ms_played) AS min_play_ms_played
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.min_play_ms_played
    FROM joined_ranges s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
)
-- Final projection
SELECT
    id,
    played_at,
    prior_rows_7d,
    prior_ms_played_7d,
    min_play_ms_played,
    TO_DATE(played_at) AS played_at_date,
    DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
FROM summarized_rows
ORDER BY days_since_played_at DESC, played_at_date DESC
