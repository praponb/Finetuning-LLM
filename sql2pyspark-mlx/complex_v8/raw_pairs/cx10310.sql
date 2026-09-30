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
        DATE_TRUNC('DAY', played_at) AS played_at_day,
        CASE WHEN source IN ('playlist', 'radio') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE is_skipped = FALSE
      AND source = 'playlist'
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, COUNT(*) AS play_count
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count
    FROM filtered_plays s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(play_count / NULLIF(SUM(play_count) OVER (PARTITION BY track_id, is_skipped), 0), 4) AS play_count_share_of_track_id_is_skipped,
        SUM(ms_played) OVER (PARTITION BY track_id, is_skipped) AS track_id_is_skipped_sum_ms_played
    FROM with_counts
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT listener_id, source_group, ms_played
    FROM metrics
) src
PIVOT (
    MAX(ms_played)
    FOR source_group IN ('group_a', 'other', 'group_b')
)
ORDER BY listener_id
