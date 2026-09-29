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
        source,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
    WHERE device = 'phone'
      AND listener_id IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        ms_played,
        device,
        is_skipped,
        device_or_unknown,
        DENSE_RANK() OVER w_device_by_ms_played AS ms_played_dense_rank,
        MAX(ms_played) OVER (PARTITION BY source) AS source_max_ms_played,
        ms_played - LAG(ms_played) OVER w_device_by_ms_played AS ms_played_change,
        SIZE(COLLECT_SET(track_id) OVER w_source) AS distinct_track_id_in_source,
        COUNT(*) OVER (PARTITION BY source) AS rows_in_source,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_source, 0), 4) AS ms_played_share_of_source
    FROM base_plays
    WINDOW w_device_by_ms_played AS (PARTITION BY device ORDER BY ms_played DESC, id), w_source AS (PARTITION BY source)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced
    WHERE ms_played_change > (SELECT AVG(ms_played_change) FROM sequenced)
)
-- Final projection
SELECT
    id,
    track_id,
    ms_played,
    device,
    device_or_unknown,
    source_max_ms_played,
    ms_played_change,
    distinct_track_id_in_source,
    rows_in_source,
    ms_played_share_of_source
FROM screened
ORDER BY device_or_unknown DESC, distinct_track_id_in_source DESC
