WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        ABS(ms_played - 223828) AS ms_played_distance
    FROM plays
    WHERE track_id <= 16
      AND played_at >= TIMESTAMP '2025-02-06 15:00:00'
),
enriched_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.device_or_unknown,
        s.ms_played_distance,
        l.plan AS listener_plan,
        l.interests AS listener_interests,
        t.release_date AS track_release_date,
        t.bpm AS track_bpm
    FROM base_plays s
    JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
    WHERE SIZE(l.interests) >= 2
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        device,
        device_or_unknown,
        ms_played_distance,
        listener_plan,
        listener_interests,
        NTILE(3) OVER w_all_by_ms_played_distance AS ms_played_distance_tercile,
        ROUND(AVG(track_bpm) OVER (PARTITION BY listener_id, track_id), 2) AS listener_id_track_id_avg_track_bpm,
        COUNT(*) OVER w_listener_id_track_id AS rows_in_listener_id_track_id,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', track_release_date), listener_id), 0), 4) AS track_bpm_share_of_track_release_date_month_list,
        NTILE(4) OVER (ORDER BY track_bpm, id) AS track_bpm_quartile,
        RANK() OVER (ORDER BY track_bpm, id) AS track_bpm_rank,
        SUM(track_bpm) OVER (PARTITION BY MONTH(track_release_date) ORDER BY ms_played_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_track_bpm,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER (PARTITION BY listener_id, track_id), 0), 4) AS track_bpm_share_of_listener_id_track_id,
        DATEDIFF(track_release_date, LAG(track_release_date) OVER (ORDER BY ms_played_distance DESC, id)) AS days_since_prev_track_release_date,
        MAX(track_bpm) OVER (PARTITION BY MONTH(track_release_date) ORDER BY ms_played_distance DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS track_bpm_max_7,
        ROUND(AVG(ms_played_distance) OVER (PARTITION BY track_id, listener_plan ORDER BY track_release_date DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_distance_avg_2_prior,
        DENSE_RANK() OVER (PARTITION BY MONTH(track_release_date) ORDER BY ms_played_distance DESC) AS ms_played_distance_dense_rank,
        DENSE_RANK() OVER w_device_listener_id_by_track AS track_bpm_dense_rank
    FROM enriched_listener
    WINDOW w_all_by_ms_played_distance AS (ORDER BY ms_played_distance DESC, id), w_listener_id_track_id AS (PARTITION BY listener_id, track_id), w_device_listener_id_by_track AS (PARTITION BY device, listener_id ORDER BY track_bpm DESC)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        listener_id,
        track_id,
        device,
        device_or_unknown,
        ms_played_distance,
        listener_plan,
        listener_interests,
        ms_played_distance_tercile,
        listener_id_track_id_avg_track_bpm,
        rows_in_listener_id_track_id,
        track_bpm_share_of_track_release_date_month_list,
        track_bpm_quartile,
        track_bpm_rank,
        running_track_bpm,
        track_bpm_share_of_listener_id_track_id,
        days_since_prev_track_release_date,
        track_bpm_max_7,
        ms_played_distance_avg_2_prior,
        ms_played_distance_dense_rank
    FROM ranked
    WHERE track_bpm_dense_rank = 1
      AND ms_played_distance_dense_rank BETWEEN 3 AND 8
),
ranked_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(rows_in_listener_id_track_id) OVER (ORDER BY ms_played_distance NULLS LAST, id) AS prev_rows_in_listener_id_track_id,
        NTH_VALUE(running_track_bpm, 2) OVER (ORDER BY rows_in_listener_id_track_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_running_track_bpm,
        DENSE_RANK() OVER (PARTITION BY ms_played_distance_dense_rank ORDER BY track_bpm_rank ASC, id) AS track_bpm_rank_dense_rank,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY track_id, device)) AS distinct_listener_id_in_track_id_device,
        DENSE_RANK() OVER (ORDER BY ms_played_distance_dense_rank) AS ms_played_distance_dense_rank_dense_rank,
        ROUND(track_bpm_max_7 / NULLIF(SUM(track_bpm_max_7) OVER w_track_bpm_rank_ms_played_dis, 0), 4) AS track_bpm_max_7_share_of_track_bpm_rank_ms_playe,
        MIN(ms_played_distance_tercile) OVER w_track_bpm_rank_ms_played_dis AS track_bpm_rank_ms_played_distance_tercile_min_ms
    FROM leaders
    WINDOW w_track_bpm_rank_ms_played_dis AS (PARTITION BY track_bpm_rank, ms_played_distance_tercile)
),
grouped AS (
    -- Summarise per group
    SELECT
        listener_plan,
        track_bpm_rank,
        distinct_listener_id_in_track_id_device,
        COUNT(*) FILTER (WHERE listener_plan IS NOT NULL) AS n_listener_plan_is_not_null,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        MAX(track_bpm_max_7_share_of_track_bpm_rank_ms_playe) AS max_track_bpm_max_7_share_of_track_bpm_rank_ms_p,
        COUNT(DISTINCT device) AS distinct_devices
    FROM ranked_2
    GROUP BY listener_plan, track_bpm_rank, distinct_listener_id_in_track_id_device
)
-- Final projection
SELECT
    listener_plan,
    track_bpm_rank,
    distinct_listener_id_in_track_id_device,
    device_list,
    max_track_bpm_max_7_share_of_track_bpm_rank_ms_p,
    distinct_devices
FROM grouped
