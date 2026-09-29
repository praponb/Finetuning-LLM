WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        DATE_TRUNC('QUARTER', played_at) AS played_at_quarter,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-02-06 09:00:00'
),
combined_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.played_at_quarter,
        s.device_prefix,
        l.country AS listener_country,
        t.release_date AS track_release_date
    FROM scoped s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.device_prefix,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.device AS play_device,
        p.is_skipped AS play_is_skipped
    FROM combined_details_listener s
    JOIN plays p ON p.listener_id = s.listener_id AND p.is_skipped = TRUE
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        ms_played,
        device,
        device_prefix,
        play_id,
        play_ms_played,
        play_device,
        CASE WHEN ms_played >= 287769 THEN 'HIGH' WHEN ms_played >= 73335 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier,
        CONCAT_WS('-', device, device_prefix) AS device_device_prefix_label,
        ABS(play_ms_played - 223262) AS play_ms_played_distance
    FROM with_children_plays
    WHERE device_prefix IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT play_id, play_device, ms_played_tier, ms_played
    FROM scored
) src
PIVOT (
    SUM(ms_played) AS total, MAX(ms_played) AS max_v
    FOR ms_played_tier IN ('HIGH', 'LOW', 'MEDIUM')
)
ORDER BY play_id, play_device
