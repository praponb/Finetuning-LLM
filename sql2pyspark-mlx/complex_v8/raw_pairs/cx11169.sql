WITH recent_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
    FROM tracks
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.moods,
        s.is_explicit_flag,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM recent_tracks s
    JOIN plays p ON p.track_id = s.id
),
play_device_stats AS (
    -- Avg duration_sec per play_device
    SELECT play_device, AVG(duration_sec) AS avg_duration_sec
    FROM line_level
    GROUP BY play_device
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_duration_sec,
        ROUND(s.duration_sec - g.avg_duration_sec, 2) AS duration_sec_minus_avg
    FROM line_level s
    JOIN play_device_stats g ON g.play_device = s.play_device
    WHERE s.duration_sec > g.avg_duration_sec
),
latest AS (
    -- Keep the latest row per play_listener_id, play_device
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        is_explicit_flag,
        play_id,
        play_listener_id,
        play_ms_played,
        play_played_at,
        play_device,
        avg_duration_sec,
        duration_sec_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY play_listener_id, play_device ORDER BY avg_duration_sec DESC, id DESC, play_id DESC) AS recency_rank
        FROM compared
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with GROUPING SETS
SELECT
    artist_id,
    is_explicit_flag,
    play_listener_id,
    MIN(play_played_at) AS first_play_played_at,
    COUNT(*) AS n_rows,
    SUM(duration_sec_minus_avg) FILTER (WHERE play_device = 'speaker') AS duration_sec_minus_avg_play_device_speaker
FROM latest
GROUP BY GROUPING SETS ((artist_id, is_explicit_flag, play_listener_id), (artist_id))
ORDER BY artist_id NULLS LAST, is_explicit_flag NULLS LAST, play_listener_id NULLS LAST
