WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled
    FROM plays
    WHERE device <> 'car'
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source,
        p.id AS play_id,
        p.device AS play_device,
        p.listener_id AS play_listener_id
    FROM prepared s
    LEFT JOIN plays p ON p.track_id = s.track_id AND p.listener_id IS NOT NULL
),
joined_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source,
        s.play_id,
        s.play_device,
        s.play_listener_id,
        l.interests AS listener_interests,
        l.signup_date AS listener_signup_date
    FROM line_level_plays s
    LEFT JOIN listeners l ON l.id = s.play_listener_id
)
-- Subtotals with GROUPING SETS
SELECT
    listener_id,
    COALESCE(play_device, 'ALL') AS play_device,
    play_id,
    GROUPING(listener_id) AS is_total_listener_id,
    COUNT(DISTINCT source) AS distinct_sources,
    COUNT(*) FILTER (WHERE play_device IN ('tv', 'desktop')) AS n_play_device_in_tv_desktop
FROM joined_listener
GROUP BY GROUPING SETS ((listener_id, play_device, play_id), (listener_id), ())
