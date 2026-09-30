WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        played_at + INTERVAL 7 DAYS AS played_at_plus_7d
    FROM plays
    WHERE source IN ('radio', 'playlist', 'search')
      AND ms_played > 145720
),
with_parent_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.ms_played_filled,
        s.played_at_plus_7d,
        t.moods AS track_moods,
        t.duration_sec AS track_duration_sec
    FROM scoped s
    JOIN tracks t ON t.id = s.track_id
),
track_id_stats AS (
    -- Max track_duration_sec per track_id
    SELECT track_id, MAX(track_duration_sec) AS max_track_duration_sec
    FROM with_parent_track
    GROUP BY track_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_track_duration_sec,
        ROUND(s.track_duration_sec - g.max_track_duration_sec, 2) AS track_duration_sec_minus_max
    FROM with_parent_track s
    JOIN track_id_stats g ON g.track_id = s.track_id
    WHERE s.track_duration_sec = g.max_track_duration_sec
)
-- Final projection
SELECT
    id,
    track_id,
    source,
    ms_played_filled,
    track_moods,
    track_duration_sec,
    track_duration_sec_minus_max,
    CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
    ROUND(max_track_duration_sec * 0.9, 2) AS max_track_duration_sec_adj
FROM compared
ORDER BY track_duration_sec_minus_max
