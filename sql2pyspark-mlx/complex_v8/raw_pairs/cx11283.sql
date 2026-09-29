WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
    WHERE ms_played > 73335
      AND played_at >= TIMESTAMP '2025-01-13 11:00:00'
),
combined_details_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.source,
        s.device_or_unknown,
        t.duration_sec AS track_duration_sec,
        t.bpm AS track_bpm,
        l.interests AS listener_interests
    FROM filtered_plays s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(CASE WHEN device <> 'car' THEN 1 END) AS play_device_count,
        COUNT(CASE WHEN source = 'search' THEN 1 END) AS play_source_count,
        MIN(ms_played) AS min_play_ms_played
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_device_count, 0) AS play_device_count,
        COALESCE(p.play_source_count, 0) AS play_source_count,
        p.min_play_ms_played
    FROM combined_details_track s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
)
-- Subtotals with GROUPING SETS
SELECT
    is_skipped,
    play_device_count,
    SUM(CASE WHEN NOT is_skipped THEN play_device_count ELSE 0 END) AS play_device_count_not_is_skipped,
    MIN(played_at) AS first_played_at,
    COUNT(*) AS cnt
FROM with_counts
GROUP BY GROUPING SETS ((is_skipped, play_device_count), (is_skipped), ())
ORDER BY is_skipped NULLS LAST, play_device_count NULLS LAST
