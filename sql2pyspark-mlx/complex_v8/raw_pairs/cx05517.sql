WITH in_window AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.ms_played) AS prior_ms_played_1d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(*) AS play_count,
        COUNT(*) FILTER (WHERE device = 'phone') AS play_device_count
    FROM plays
    GROUP BY listener_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_device_count, 0) AS play_device_count
    FROM in_window s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
)
SELECT
    id,
    listener_id,
    played_at,
    prior_rows_1d,
    prior_ms_played_1d,
    play_count,
    play_device_count,
    play_device_count + play_count AS play_device_count_plus_play_count
FROM summarized_rows
WHERE prior_ms_played_1d < 1200000
ORDER BY played_at, play_count
