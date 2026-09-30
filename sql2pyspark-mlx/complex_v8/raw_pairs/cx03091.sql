WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE (track_id IS NOT NULL OR device = 'phone')
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY is_skipped, track_id) AS rows_in_is_skipped_track_id,
        FIRST_VALUE(played_at) OVER (PARTITION BY track_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY is_skipped, track_id)) AS distinct_listener_id_in_is_skipped_track_id,
        LAG(ms_played, 3) OVER w_track_id_by_ms_played AS prev3_ms_played,
        NTILE(5) OVER (PARTITION BY track_id, is_skipped ORDER BY played_at DESC, id) AS played_at_quintile,
        LEAD(ms_played) OVER (PARTITION BY is_skipped ORDER BY ms_played, id) AS next_ms_played,
        SIZE(COLLECT_SET(source) OVER w_is_skipped_track_id) AS distinct_source_in_is_skipped_track_id,
        NTILE(3) OVER (PARTITION BY track_id ORDER BY ms_played, id) AS ms_played_tercile
    FROM filtered
    WINDOW w_track_id_by_ms_played AS (PARTITION BY track_id ORDER BY ms_played, id), w_is_skipped_track_id AS (PARTITION BY is_skipped, track_id)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    source,
    rows_in_is_skipped_track_id,
    distinct_listener_id_in_is_skipped_track_id,
    prev3_ms_played,
    ABS(ms_played_tercile - 6) AS ms_played_tercile_distance,
    COALESCE(next_ms_played, 0) AS next_ms_played_filled
FROM with_trends
ORDER BY distinct_listener_id_in_is_skipped_track_id, id
LIMIT 5
