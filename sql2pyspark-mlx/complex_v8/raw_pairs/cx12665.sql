WITH prepared AS (
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
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket_2
    FROM plays
    WHERE device IN ('tv', 'phone', 'speaker')
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND TO_DATE(p.played_at) < DATE '2025-03-08'
        )
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label,
        YEAR(played_at) AS played_at_year,
        CONCAT(source, '#', CAST(ms_played_bucket AS STRING)) AS source_ref
    FROM with_activity
    WHERE listener_id IS NOT NULL
),
latest AS (
    -- Keep the latest row per source, track_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_bucket,
        ms_played_bucket_2,
        played_at_label,
        played_at_year,
        source_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, track_id ORDER BY played_at DESC, id DESC) AS row_num
        FROM labeled
    ) ranked
    WHERE row_num = 1
)
SELECT
    COALESCE(source, 'ALL') AS source,
    track_id,
    listener_id,
    COUNT(*) FILTER (WHERE source IS NOT NULL) AS n_source_is_not_null,
    ARRAY_SORT(COLLECT_SET(source)) AS source_list
FROM latest
GROUP BY GROUPING SETS ((source, track_id, listener_id), (source))
ORDER BY source NULLS LAST, track_id NULLS LAST, listener_id NULLS LAST
