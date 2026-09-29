WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(source), 1, 2) AS source_prefix,
        CONCAT_WS(' / ', source, device) AS source_device_label
    FROM plays
    WHERE source = 'playlist'
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        NTILE(3) OVER (ORDER BY ms_played DESC, id) AS ms_played_tercile,
        DENSE_RANK() OVER (ORDER BY ms_played DESC, id) AS ms_played_dense_rank,
        LAG(ms_played, 3, 0) OVER w_all_by_ms_played AS prev3_ms_played
    FROM base_plays
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played DESC, id)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        device,
        ms_played_tercile,
        ms_played_dense_rank,
        prev3_ms_played,
        ROW_NUMBER() OVER w_listener_id_ms_played_tercil AS ms_played_tercile_seq,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY is_skipped)) AS distinct_source_in_is_skipped
    FROM windowed
    WINDOW w_listener_id_ms_played_tercil AS (PARTITION BY listener_id, ms_played_tercile ORDER BY ms_played_tercile NULLS LAST, id)
)
-- Subtotals with ROLLUP
SELECT
    device,
    track_id,
    ARRAY_SORT(COLLECT_SET(device)) AS device_list
FROM with_trends
GROUP BY ROLLUP(device, track_id)
