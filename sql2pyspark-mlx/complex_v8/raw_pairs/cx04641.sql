WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT(source, '#', CAST(track_id AS STRING)) AS source_ref
    FROM plays
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(5) OVER w_is_skipped_listener_id_by_ms AS ms_played_quintile,
        FIRST_VALUE(ms_played) OVER (PARTITION BY is_skipped, listener_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        SUM(ms_played) OVER (PARTITION BY is_skipped, device) AS is_skipped_device_sum_ms_played,
        LEAD(ms_played) OVER (PARTITION BY is_skipped, listener_id ORDER BY ms_played DESC, id) AS next_ms_played,
        RANK() OVER (PARTITION BY is_skipped, listener_id ORDER BY ms_played DESC, id) AS ms_played_rank
    FROM scoped_plays
    WINDOW w_is_skipped_listener_id_by_ms AS (PARTITION BY is_skipped, listener_id ORDER BY ms_played DESC, id)
),
latest AS (
    -- Keep the latest row per device, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        source_ref,
        ms_played_quintile,
        first_ms_played,
        is_skipped_device_sum_ms_played,
        next_ms_played,
        ms_played_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device, listener_id ORDER BY first_ms_played DESC, id DESC) AS rn
        FROM ranked
    ) ranked
    WHERE rn = 1
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(device, 'ALL') AS device,
    COALESCE(source, 'ALL') AS source,
    GROUPING_ID() AS gid,
    ROUND(SUM(first_ms_played) / NULLIF(SUM(next_ms_played), 0), 3) AS first_ms_played_to_next_ms_played_ratio,
    ARRAY_SORT(COLLECT_SET(source)) AS source_list
FROM latest
GROUP BY GROUPING SETS ((device, source), (device), ())
ORDER BY device NULLS LAST, source NULLS LAST
