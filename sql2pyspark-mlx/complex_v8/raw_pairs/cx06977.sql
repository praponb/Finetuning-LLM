WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device
    FROM plays
    WHERE NOT is_skipped
),
listener_id_stats AS (
    -- Max ms_played per listener_id
    SELECT listener_id, MAX(ms_played) AS max_ms_played
    FROM prepared_plays
    GROUP BY listener_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_ms_played,
        ROUND(s.ms_played - g.max_ms_played, 2) AS ms_played_minus_max
    FROM prepared_plays s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.ms_played = g.max_ms_played
),
detail_rows AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.max_ms_played,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source,
        p.device AS play_device,
        p.played_at AS play_played_at
    FROM above_average s
    JOIN plays p ON p.listener_id = s.listener_id AND p.is_skipped = TRUE
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        ms_played,
        max_ms_played,
        play_id,
        play_source,
        SUBSTRING(TRIM(play_device), 1, 3) AS play_device_prefix,
        ROUND((UNIX_TIMESTAMP(play_played_at) - UNIX_TIMESTAMP(played_at)) / 3600, 1) AS hours_played_at_to_play_played_at
    FROM detail_rows
),
play_source_stats AS (
    -- Avg hours_played_at_to_play_played_at per play_source
    SELECT play_source, AVG(hours_played_at_to_play_played_at) AS avg_hours_played_at_to_play_played_at
    FROM derived
    GROUP BY play_source
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_hours_played_at_to_play_played_at,
        ROUND(s.hours_played_at_to_play_played_at - g.avg_hours_played_at_to_play_played_at, 2) AS hours_played_at_to_play_played_at_minus_avg
    FROM derived s
    JOIN play_source_stats g ON g.play_source = s.play_source
    WHERE s.hours_played_at_to_play_played_at > g.avg_hours_played_at_to_play_played_at
),
enriched_listener AS (
    -- Attach listener attributes
    SELECT
        s.*,
        l.plan AS listener_plan,
        l.country AS listener_country,
        p.ms_played AS play_ms_played,
        p.device AS play_device
    FROM vs_group s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN plays p ON p.id = s.play_id
)
SELECT
    id,
    listener_id,
    max_ms_played,
    play_id,
    play_source,
    hours_played_at_to_play_played_at,
    avg_hours_played_at_to_play_played_at,
    hours_played_at_to_play_played_at_minus_avg,
    listener_plan,
    listener_country,
    play_ms_played,
    play_device,
    COALESCE(ms_played, 0) AS ms_played_filled
FROM enriched_listener
ORDER BY max_ms_played, hours_played_at_to_play_played_at DESC
