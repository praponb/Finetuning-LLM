WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        COALESCE(ms_played, 0) AS ms_played_filled,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
    WHERE listener_id IS NOT NULL
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        device_or_unknown,
        ROUND(ms_played_filled - AVG(ms_played_filled) OVER (PARTITION BY track_id, is_skipped), 2) AS ms_played_filled_vs_track_id_is_skipped_avg,
        NTILE(4) OVER (ORDER BY ms_played, id) AS ms_played_quartile,
        MIN(ms_played_filled) OVER (PARTITION BY track_id, is_skipped) AS track_id_is_skipped_min_ms_played_filled,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY track_id ORDER BY ms_played_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY ms_played, id), 3) AS ms_played_percent_rank,
        SIZE(COLLECT_SET(device_or_unknown) OVER (PARTITION BY track_id, is_skipped)) AS distinct_device_or_unknown_in_track_id_is_skippe,
        NTILE(3) OVER w_all_by_ms_played_filled AS ms_played_filled_tercile,
        SUM(ms_played_filled) OVER (PARTITION BY track_id, is_skipped) AS track_id_is_skipped_sum_ms_played_filled,
        LAST_VALUE(ms_played) OVER (PARTITION BY track_id ORDER BY ms_played_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        RANK() OVER (ORDER BY ms_played_filled DESC) AS ms_played_filled_rank,
        MAX(ms_played) OVER (PARTITION BY track_id ORDER BY ms_played_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_so_far,
        ROUND(AVG(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        SIZE(COLLECT_SET(device) OVER w_track_id_is_skipped) AS distinct_device_in_track_id_is_skipped,
        ms_played - LAG(ms_played) OVER (ORDER BY ms_played, id) AS ms_played_change,
        SUM(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ms_played_sum_4
    FROM recent_plays
    WINDOW w_all_by_ms_played_filled AS (ORDER BY ms_played_filled DESC, id), w_track_id_is_skipped AS (PARTITION BY track_id, is_skipped)
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    ms_played,
    device,
    is_skipped,
    ms_played_quartile,
    track_id_is_skipped_min_ms_played_filled,
    second_played_at,
    ms_played_percent_rank,
    distinct_device_or_unknown_in_track_id_is_skippe,
    ms_played_filled_tercile,
    track_id_is_skipped_sum_ms_played_filled,
    max_ms_played_so_far,
    ms_played_avg_2,
    distinct_device_in_track_id_is_skipped,
    ms_played_change,
    DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at,
    CASE WHEN ms_played_quartile >= 9 THEN 'HIGH' WHEN ms_played_quartile >= 4 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_quartile_tier
FROM metrics
WHERE ms_played_filled_rank > 2
ORDER BY device DESC
