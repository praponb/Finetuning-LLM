WITH prepared_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        MONTH(played_at) AS played_at_month,
        played_at + INTERVAL 30 MINUTES AS played_at_plus_30m,
        CONCAT(device, '#', CAST(track_id AS STRING)) AS device_ref
    FROM plays
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.played_at_month,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.source AS play_source,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at
    FROM prepared_plays s
    JOIN plays p ON p.track_id = s.track_id
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(play_played_at, 'yyyy-MM') AS play_played_at_ym,
        played_at_month,
        source,
        SUM(ms_played) AS total_ms_played,
        SUM(CASE WHEN play_is_skipped = TRUE THEN play_ms_played ELSE 0 END) AS play_ms_played_play_is_skipped_true,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list
    FROM with_children
    WHERE play_played_at >= TIMESTAMP '2025-01-31 12:00:00'
    GROUP BY 1, 2, 3
)
SELECT
    play_played_at_ym,
    played_at_month,
    total_ms_played,
    play_ms_played_play_is_skipped_true,
    source_list,
    played_at_month - total_ms_played AS played_at_month_minus_total_ms_played,
    CASE WHEN played_at_month >= 10 THEN 'large' WHEN played_at_month >= 4 THEN 'medium' ELSE 'small' END AS played_at_month_tier
FROM totals
WHERE source IN ('search', 'album')
ORDER BY played_at_month_minus_total_ms_played DESC
