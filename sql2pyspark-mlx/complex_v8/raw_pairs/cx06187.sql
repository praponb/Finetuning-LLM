WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        UPPER(TRIM(device)) AS device_clean
    FROM plays
    WHERE track_id IS NOT NULL
      AND device IS NOT NULL
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(10) OVER w_track_id_by_played_at AS played_at_decile,
        LEAD(ms_played_filled, 2) OVER (PARTITION BY track_id ORDER BY played_at, id) AS next2_ms_played_filled,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped, track_id ORDER BY ms_played_filled NULLS LAST), 3) AS ms_played_filled_cume_dist,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY played_at, id) AS played_at_dense_rank,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY track_id ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        MAX(ms_played_filled) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_filled_so_far,
        ROUND(AVG(ms_played_filled) OVER (ORDER BY played_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ms_played_filled_avg_3,
        SUM(ms_played_filled) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played_filled
    FROM cleaned
    WINDOW w_track_id_by_played_at AS (PARTITION BY track_id ORDER BY played_at, id)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(second_played_at, 'yyyy-MM') AS second_played_at_label,
        COALESCE(running_ms_played_filled, 0) AS running_ms_played_filled_filled
    FROM sequenced
    WHERE listener_id IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY device ORDER BY max_ms_played_filled_so_far DESC, id) AS max_ms_played_filled_so_far_decile,
        ROUND(AVG(played_at_dense_rank) OVER (PARTITION BY device), 2) AS device_avg_played_at_dense_rank,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY device)) AS distinct_track_id_in_device,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY device)) AS distinct_source_in_device
    FROM tagged
)
-- Final projection
SELECT
    id,
    listener_id,
    device,
    ms_played_filled,
    played_at_decile,
    next2_ms_played_filled,
    ms_played_filled_cume_dist,
    played_at_dense_rank,
    max_ms_played_filled_so_far,
    running_ms_played_filled,
    second_played_at_label,
    max_ms_played_filled_so_far_decile,
    device_avg_played_at_dense_rank,
    distinct_track_id_in_device,
    distinct_source_in_device,
    DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at,
    COALESCE(source, 'UNKNOWN') AS source_or_unknown
FROM metrics
