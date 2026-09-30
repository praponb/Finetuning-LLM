WITH scoped AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        TO_DATE(played_at) AS played_at_date,
        ABS(ms_played - 168782) AS ms_played_distance
    FROM plays
    WHERE track_id IS NOT NULL
),
enriched AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.played_at_date,
        t.duration_sec AS track_duration_sec,
        t.is_explicit AS track_is_explicit,
        t.release_date AS track_release_date,
        l.plan AS listener_plan,
        l.username AS listener_username
    FROM scoped s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        track_id,
        track_duration_sec,
        track_is_explicit,
        listener_plan,
        listener_username,
        ROUND(track_duration_sec - AVG(track_duration_sec) OVER (PARTITION BY track_id, device), 2) AS track_duration_sec_vs_track_id_device_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_plan, device ORDER BY ms_played ASC, id), 3) AS ms_played_cume_dist,
        ROUND(track_duration_sec / NULLIF(SUM(track_duration_sec) OVER (PARTITION BY track_id), 0), 4) AS track_duration_sec_share_of_track_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id, track_is_explicit ORDER BY ms_played, id), 3) AS ms_played_percent_rank,
        ROUND(AVG(track_duration_sec) OVER w_track_id_device, 2) AS track_id_device_avg_track_duration_sec,
        LAG(track_duration_sec) OVER (PARTITION BY is_skipped ORDER BY track_release_date ASC, id) AS prev_track_duration_sec,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        ROUND(ms_played - AVG(ms_played) OVER w_track_id, 2) AS ms_played_vs_track_id_avg,
        SIZE(COLLECT_SET(device) OVER w_track_id) AS distinct_device_in_track_id,
        DENSE_RANK() OVER (PARTITION BY listener_id ORDER BY ms_played) AS ms_played_dense_rank,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id, device), 2) AS ms_played_vs_track_id_device_avg,
        NTILE(5) OVER w_is_skipped_by_track_release AS track_release_date_quintile,
        ROUND(AVG(ms_played) OVER w_track_id_device, 2) AS track_id_device_avg_ms_played,
        MAX(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ms_played_max_6_prior,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped ORDER BY track_release_date ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_6_prior
    FROM enriched
    WINDOW w_track_id_device AS (PARTITION BY track_id, device), w_track_id AS (PARTITION BY track_id), w_is_skipped_by_track_release AS (PARTITION BY is_skipped ORDER BY track_release_date ASC, id)
),
enriched_2 AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(listener_plan), 1, 4) AS listener_plan_prefix,
        CONCAT(listener_username, '#', CAST(prev_track_duration_sec AS STRING)) AS listener_username_ref,
        ROUND(ms_played_percent_rank / NULLIF(prev_track_duration_sec, 0), 3) AS ms_played_percent_rank_per_prev_track_duration_s
    FROM ranked
)
SELECT
    id,
    track_id,
    track_duration_sec,
    track_is_explicit,
    listener_username,
    ms_played_percent_rank,
    track_id_device_avg_track_duration_sec,
    prev_track_duration_sec,
    rows_in_track_id,
    ms_played_vs_track_id_avg,
    distinct_device_in_track_id,
    ms_played_dense_rank,
    ms_played_vs_track_id_device_avg,
    track_release_date_quintile,
    track_id_device_avg_ms_played,
    listener_plan_prefix,
    listener_username_ref,
    ms_played_percent_rank_per_prev_track_duration_s
FROM enriched_2
ORDER BY prev_track_duration_sec DESC
