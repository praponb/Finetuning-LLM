WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref
    FROM plays
    WHERE track_id IS NOT NULL
),
with_parent_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        l.username AS listener_username,
        t.duration_sec AS track_duration_sec
    FROM valid s
    LEFT JOIN listeners l ON l.id = s.listener_id AND l.country IS NOT NULL
    LEFT JOIN tracks t ON t.id = s.track_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.listener_username,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year
    FROM with_parent_listener s
    LEFT JOIN listeners l ON l.id = s.listener_id AND SIZE(l.interests) >= 2
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.listener_signup_date,
        p.id AS play_id,
        p.device AS play_device,
        p.source AS play_source
    FROM combined_details s
    JOIN plays p ON p.listener_id = s.listener_id
),
stats AS (
    -- Summarise per group
    SELECT
        track_id,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        ARRAY_SORT(COLLECT_SET(play_device)) AS play_device_list,
        SUM(CASE WHEN play_device = 'phone' THEN 1 ELSE 0 END) AS n_play_device_phone,
        SUM(CASE WHEN device = 'phone' THEN 1 ELSE 0 END) AS n_device_phone,
        MAX(listener_signup_date) AS last_listener_signup_date
    FROM detail_rows_plays
    GROUP BY track_id
)
-- Final projection
SELECT
    track_id,
    device_list,
    play_device_list,
    last_listener_signup_date,
    GREATEST(n_device_phone, n_play_device_phone) AS greatest_n_device_phone_n_play_device_phone,
    COALESCE(n_device_phone, 0) AS n_device_phone_filled
FROM stats
WHERE SIZE(play_device_list) >= 1
ORDER BY last_listener_signup_date DESC NULLS FIRST
