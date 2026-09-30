WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        CONCAT(device, '#', CAST(id AS STRING)) AS device_ref,
        CASE WHEN source IN ('playlist', 'album') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE device IN ('desktop', 'tv', 'car')
      AND track_id >= 21
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        device_ref,
        source_group,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY listener_id), 0), 4) AS ms_played_share_of_listener_id,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at ASC, id))) / 60, 1) AS minutes_since_prev_played_at,
        SUM(ms_played) OVER (PARTITION BY listener_id) AS listener_id_sum_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS listener_id_avg_ms_played,
        LAG(ms_played) OVER (PARTITION BY device, source_group ORDER BY played_at DESC, id) AS prev_ms_played
    FROM scoped
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.device_ref,
        s.ms_played_share_of_listener_id,
        s.minutes_since_prev_played_at,
        s.listener_id_sum_ms_played,
        s.listener_id_avg_ms_played,
        s.prev_ms_played,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at
    FROM with_trends s
    LEFT JOIN plays p ON p.listener_id = s.listener_id
)
SELECT
    play_source,
    play_is_skipped,
    play_id,
    GROUPING_ID() AS gid,
    COUNT(*) AS cnt,
    COUNT(*) FILTER (WHERE play_device IN ('desktop', 'phone')) AS n_play_device_in_desktop_phone
FROM exploded_detail
GROUP BY CUBE(play_source, play_is_skipped, play_id)
ORDER BY play_source NULLS LAST, play_is_skipped NULLS LAST, play_id NULLS LAST
