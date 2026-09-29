WITH base_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
),
with_prev AS (
    -- Previous played_at per source
    SELECT *, LAG(played_at) OVER (PARTITION BY source ORDER BY played_at, id) AS prev_played_at
    FROM base_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 120 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.track_id AS play_track_id
    FROM sessionized s
    JOIN plays p ON p.listener_id = s.listener_id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        source,
        MAX(ms_played) AS max_ms_played,
        ARRAY_SORT(COLLECT_SET(play_source)) AS play_source_list,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        ROUND(AVG(ms_played), 2) AS avg_ms_played,
        SUM(ms_played) AS total_ms_played
    FROM exploded_detail
    WHERE device = 'speaker'
    GROUP BY source
)
-- Final projection
SELECT
    source,
    max_ms_played,
    play_source_list,
    distinct_listener_ids,
    avg_ms_played,
    total_ms_played,
    ABS(max_ms_played - 123500) AS max_ms_played_distance,
    CONCAT(source, '#', CAST(max_ms_played AS STRING)) AS source_ref
FROM summary
WHERE max_ms_played BETWEEN 24750 AND 261750
ORDER BY max_ms_played_distance, source_ref DESC
