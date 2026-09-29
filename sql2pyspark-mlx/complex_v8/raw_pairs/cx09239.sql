WITH scoped AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE source NOT IN ('radio', 'playlist')
      AND played_at < TIMESTAMP '2025-02-18 12:00:00'
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.days_since_played_at,
        p.id AS play_id,
        p.device AS play_device,
        p.track_id AS play_track_id,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped
    FROM scoped s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(days_since_played_at / NULLIF(SUM(days_since_played_at) OVER w_play_device, 0), 4) AS days_since_played_at_share_of_play_device,
        LEAD(played_at) OVER (ORDER BY play_ms_played, id, play_id) AS next_played_at
    FROM line_level
    WINDOW w_play_device AS (PARTITION BY play_device)
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.days_since_played_at,
        s.play_id,
        s.play_track_id,
        s.play_ms_played,
        s.play_is_skipped,
        s.next_played_at,
        p.id AS play_id_2,
        p.device AS play_device_2,
        p.ms_played AS play_ms_played_2,
        p.played_at AS play_played_at,
        p.track_id AS play_track_id_2
    FROM ranked s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    days_since_played_at,
    play_id,
    play_is_skipped,
    next_played_at,
    play_id_2,
    play_played_at,
    play_track_id_2,
    YEAR(next_played_at) AS next_played_at_year,
    DATEDIFF(DATE '2026-01-01', play_played_at) AS days_since_play_played_at
FROM line_level_plays
