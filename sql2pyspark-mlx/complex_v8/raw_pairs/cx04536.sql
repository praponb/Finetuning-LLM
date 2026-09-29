WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        ABS(ms_played - 225733) AS ms_played_distance,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        DATE_TRUNC('WEEK', played_at) AS played_at_week
    FROM plays
    WHERE (source <> 'search' OR device = 'tv')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        ms_played_distance,
        device_or_unknown,
        played_at_week,
        COUNT(*) OVER w_track_id AS rows_in_track_id,
        MAX(ms_played_distance) OVER (PARTITION BY is_skipped, MONTH(played_at) ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_distance_so_far
    FROM recent_plays
    WINDOW w_track_id AS (PARTITION BY track_id)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY played_at), 3) AS played_at_cume_dist,
        SIZE(COLLECT_SET(device_or_unknown) OVER w_listener_id_source) AS distinct_device_or_unknown_in_listener_id_source,
        COUNT(*) OVER (PARTITION BY listener_id, source) AS rows_in_listener_id_source,
        ROUND(ms_played_distance / NULLIF(SUM(ms_played_distance) OVER (PARTITION BY listener_id, source), 0), 4) AS ms_played_distance_share_of_listener_id_source,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY listener_id, device_or_unknown ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played
    FROM ranked
    WINDOW w_listener_id_source AS (PARTITION BY listener_id, source)
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count,
        (SELECT SUM(p.ms_played) FROM plays p WHERE p.listener_id = s.listener_id) AS total_play_ms_played
    FROM windowed s
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_device_or_unknown_in_listener_id_source,
    play_count,
    is_skipped,
    ROUND(100 * COUNT(CASE WHEN source = 'radio' THEN 1 END) / COUNT(*), 1) AS pct_source_radio,
    COUNT(DISTINCT listener_id) AS distinct_listener_ids
FROM profiled
GROUP BY GROUPING SETS ((distinct_device_or_unknown_in_listener_id_source, play_count, is_skipped), (distinct_device_or_unknown_in_listener_id_source))
ORDER BY distinct_device_or_unknown_in_listener_id_source NULLS LAST, play_count NULLS LAST, is_skipped NULLS LAST
