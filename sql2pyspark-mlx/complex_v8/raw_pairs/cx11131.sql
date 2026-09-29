WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        DATE_TRUNC('DAY', played_at) AS played_at_day
    FROM plays
    WHERE device = 'car'
      AND NOT is_skipped
),
deduped AS (
    -- Keep the latest row per listener_id, track_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        source_or_unknown,
        ms_played_bucket,
        played_at_day
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id, track_id ORDER BY played_at DESC, id DESC) AS rn
        FROM prepared_plays
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        source,
        source_or_unknown,
        played_at_day,
        LAST_VALUE(ms_played_bucket) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY ms_played_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played_bucket,
        ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played_bucket DESC, id) AS ms_played_bucket_seq,
        ROUND(AVG(ms_played_bucket) OVER (PARTITION BY listener_id), 2) AS listener_id_avg_ms_played_bucket,
        ROUND((UNIX_TIMESTAMP(played_at_day) - UNIX_TIMESTAMP(LAG(played_at_day) OVER (ORDER BY played_at DESC, id))) / 60, 1) AS minutes_since_prev_played_at_day
    FROM deduped
)
SELECT
    listener_id,
    source_or_unknown,
    source,
    GROUPING_ID() AS gid,
    ARRAY_SORT(COLLECT_SET(source)) AS source_list,
    ROUND(SUM(minutes_since_prev_played_at_day) / NULLIF(SUM(ms_played), 0), 3) AS minutes_since_prev_played_at_day_to_ms_played_ra,
    COUNT(*) AS cnt
FROM ranked
GROUP BY GROUPING SETS ((listener_id, source_or_unknown, source), (listener_id), ())
