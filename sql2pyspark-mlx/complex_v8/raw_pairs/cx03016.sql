WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        CASE WHEN source IN ('playlist', 'album') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count
    FROM filtered s
)
SELECT id, field, metric_value
FROM (
    SELECT id, play_count, ms_played
    FROM with_stats
) src
UNPIVOT (
    metric_value FOR field IN (play_count, ms_played)
) unp
