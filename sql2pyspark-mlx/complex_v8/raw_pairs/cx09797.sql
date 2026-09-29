WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        CONCAT_WS('-', device, source) AS device_source_label,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        device_source_label,
        source_len,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY is_skipped, track_id)) AS distinct_listener_id_in_is_skipped_track_id,
        RANK() OVER (PARTITION BY is_skipped, source ORDER BY ms_played) AS ms_played_rank
    FROM base_plays
),
scored AS (
    -- Label and score rows
    SELECT *, SUBSTRING(TRIM(source), 1, 3) AS source_prefix
    FROM metrics
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT track_id, source, source_len
    FROM scored
) src
PIVOT (
    SUM(source_len)
    FOR source IN ('radio', 'search')
)
ORDER BY track_id
