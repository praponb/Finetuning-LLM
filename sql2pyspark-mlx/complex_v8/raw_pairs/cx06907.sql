WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE source IN ('search', 'radio', 'playlist')
      AND device <> 'car'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        device_or_unknown,
        source_slug,
        ROUND(AVG(ms_played) OVER (PARTITION BY device_or_unknown ORDER BY ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_1_prior,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id, device), 2) AS ms_played_vs_listener_id_device_avg,
        RANK() OVER w_device_or_unknown_by_ms_play_2 AS ms_played_rank,
        ROW_NUMBER() OVER (PARTITION BY is_skipped, device_or_unknown ORDER BY ms_played DESC, id) AS seq_no,
        NTILE(4) OVER (PARTITION BY is_skipped, device_or_unknown ORDER BY ms_played DESC, id) AS ms_played_quartile,
        ROW_NUMBER() OVER w_device_is_skipped_by_ms_play AS rn,
        ROUND(AVG(ms_played) OVER (PARTITION BY device_or_unknown ORDER BY ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7,
        NTILE(5) OVER (PARTITION BY device, is_skipped ORDER BY ms_played ASC NULLS LAST, id) AS ms_played_quintile,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY source, is_skipped ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        FIRST_VALUE(ms_played) OVER (PARTITION BY device, is_skipped ORDER BY ms_played ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY source, is_skipped ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played
    FROM filtered
    WINDOW w_device_or_unknown_by_ms_play_2 AS (PARTITION BY device_or_unknown ORDER BY ms_played DESC, id), w_device_is_skipped_by_ms_play AS (PARTITION BY device, is_skipped ORDER BY ms_played ASC NULLS LAST, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        listener_id,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        COUNT(DISTINCT device) AS distinct_devices,
        COUNT(*) AS cnt,
        ROUND(SUM(ms_played_avg_7), 2) AS total_ms_played_avg_7
    FROM sequenced
    GROUP BY listener_id
    HAVING MAX(ms_played_avg_1_prior) > 84000.0
)
SELECT
    listener_id,
    device_list,
    distinct_devices,
    cnt
FROM summary
