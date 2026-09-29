WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM plays
    WHERE track_id IS NOT NULL
      AND ms_played <= 340114
),
combined_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        t.is_explicit AS track_is_explicit,
        t.duration_sec AS track_duration_sec
    FROM prepared s
    JOIN tracks t ON t.id = s.track_id AND NOT t.is_explicit
    WHERE t.duration_sec >= 144
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(track_duration_sec) OVER (ORDER BY track_duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS track_duration_sec_avg_6_prior,
        DENSE_RANK() OVER (PARTITION BY device, is_skipped ORDER BY track_duration_sec) AS track_duration_sec_dense_rank,
        SUM(track_duration_sec) OVER (ORDER BY track_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_track_duration_sec,
        MIN(track_duration_sec) OVER (PARTITION BY track_is_explicit) AS track_is_explicit_min_track_duration_sec,
        track_duration_sec - LAG(track_duration_sec) OVER (PARTITION BY track_id ORDER BY track_duration_sec ASC, id) AS track_duration_sec_change,
        SUM(track_duration_sec) OVER (PARTITION BY track_id ORDER BY track_duration_sec ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS track_duration_sec_sum_3_prior,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY track_duration_sec ASC) AS track_duration_sec_dense_rank_2,
        LAST_VALUE(track_duration_sec) OVER (PARTITION BY track_id ORDER BY track_duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_track_duration_sec
    FROM combined_details_track
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN device IN ('desktop', 'tv') THEN 'group_a' WHEN device = 'phone' THEN 'group_b' ELSE 'other' END AS device_group,
        FLOOR(last_track_duration_sec / 50) * 50 AS last_track_duration_sec_bucket
    FROM metrics
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        running_track_duration_sec - LAG(running_track_duration_sec) OVER (PARTITION BY TO_DATE(played_at), device ORDER BY track_duration_sec_sum_3_prior DESC, id) AS running_track_duration_sec_change,
        MIN(last_track_duration_sec) OVER (PARTITION BY track_duration_sec_dense_rank_2, device ORDER BY track_duration_sec_dense_rank_2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_last_track_duration_sec_so_far,
        SUM(last_track_duration_sec) OVER (PARTITION BY track_is_explicit, is_skipped ORDER BY track_duration_sec_dense_rank_2 ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last_track_duration_sec_sum_7
    FROM scored
),
totals AS (
    -- Group-level totals
    SELECT
        listener_id,
        track_duration_sec_dense_rank_2,
        CASE WHEN last_track_duration_sec >= 352 THEN 'HIGH' WHEN last_track_duration_sec >= 154 THEN 'MEDIUM' ELSE 'LOW' END AS last_track_duration_sec_tier,
        COUNT(DISTINCT device_group) AS distinct_device_groups,
        MIN(running_track_duration_sec_change) AS min_running_track_duration_sec_change,
        SUM(min_last_track_duration_sec_so_far) FILTER (WHERE device = 'phone') AS min_last_track_duration_sec_so_far_device_phone
    FROM sequenced
    GROUP BY listener_id, track_duration_sec_dense_rank_2, CASE WHEN last_track_duration_sec >= 352 THEN 'HIGH' WHEN last_track_duration_sec >= 154 THEN 'MEDIUM' ELSE 'LOW' END
)
-- Final projection
SELECT
    listener_id,
    last_track_duration_sec_tier,
    distinct_device_groups,
    min_last_track_duration_sec_so_far_device_phone
FROM totals
ORDER BY distinct_device_groups, listener_id DESC NULLS FIRST
