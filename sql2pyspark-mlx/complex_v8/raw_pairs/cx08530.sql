WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source = 'search'
      AND listener_id IS NOT NULL
),
with_details_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        l.plan AS listener_plan,
        l.signup_date AS listener_signup_date,
        t.release_date AS track_release_date,
        t.bpm AS track_bpm
    FROM valid_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
    JOIN tracks t ON t.id = s.track_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY device, listener_id)) AS distinct_source_in_device_listener_id,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_plan, source ORDER BY track_release_date DESC, id), 3) AS track_release_date_cume_dist,
        MAX(track_bpm) OVER (PARTITION BY listener_plan, source ORDER BY track_release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_track_bpm_so_far,
        ROUND(AVG(track_bpm) OVER (PARTITION BY source, MONTH(listener_signup_date) ORDER BY track_bpm NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS track_bpm_avg_3,
        MAX(track_bpm) OVER (PARTITION BY device, listener_id) AS device_listener_id_max_track_bpm,
        SIZE(COLLECT_SET(listener_plan) OVER (PARTITION BY device, listener_id)) AS distinct_listener_plan_in_device_listener_id,
        ROUND(track_bpm - AVG(track_bpm) OVER (PARTITION BY device, listener_id), 2) AS track_bpm_vs_device_listener_id_avg,
        ROUND(CUME_DIST() OVER (ORDER BY listener_signup_date DESC, id), 3) AS listener_signup_date_cume_dist,
        DENSE_RANK() OVER (PARTITION BY source, MONTH(listener_signup_date) ORDER BY track_bpm NULLS LAST) AS track_bpm_dense_rank
    FROM with_details_listener
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(max_track_bpm_so_far) OVER (PARTITION BY YEAR(track_release_date), device ORDER BY track_release_date_cume_dist, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS max_track_bpm_so_far_sum_4,
        LAG(track_release_date_cume_dist) OVER (PARTITION BY YEAR(track_release_date), device ORDER BY track_release_date_cume_dist, id) AS prev_track_release_date_cume_dist,
        FIRST_VALUE(track_bpm_dense_rank) OVER (PARTITION BY YEAR(track_release_date), device ORDER BY track_release_date_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_track_bpm_dense_rank
    FROM windowed
)
SELECT
    id,
    track_id,
    device,
    is_skipped,
    source,
    listener_signup_date,
    track_release_date,
    track_bpm,
    distinct_source_in_device_listener_id,
    track_release_date_cume_dist,
    max_track_bpm_so_far,
    track_bpm_avg_3,
    distinct_listener_plan_in_device_listener_id,
    track_bpm_vs_device_listener_id_avg,
    listener_signup_date_cume_dist,
    track_bpm_dense_rank,
    max_track_bpm_so_far_sum_4,
    DATEDIFF(listener_signup_date, track_release_date) AS days_track_release_date_to_listener_signup_date
FROM with_trends
