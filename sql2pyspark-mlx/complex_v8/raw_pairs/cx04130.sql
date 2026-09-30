WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE track_id IS NOT NULL
      AND device = 'car'
),
with_details_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        l.plan AS listener_plan,
        l.interests AS listener_interests
    FROM filtered s
    JOIN listeners l ON l.id = s.listener_id AND l.signup_date < DATE '2024-12-03'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY device), 0), 4) AS ms_played_share_of_device,
        LAG(ms_played) OVER (PARTITION BY is_skipped, listener_plan ORDER BY ms_played DESC, id) AS prev_ms_played,
        ROW_NUMBER() OVER (PARTITION BY device, listener_id ORDER BY ms_played DESC, id) AS ms_played_seq,
        ms_played - LAG(ms_played) OVER (PARTITION BY is_skipped, track_id ORDER BY played_at DESC, id) AS ms_played_change,
        SUM(ms_played) OVER (PARTITION BY is_skipped, listener_plan ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        MIN(ms_played) OVER (PARTITION BY device, listener_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY device)) AS distinct_source_in_device,
        ROUND(AVG(ms_played) OVER (PARTITION BY device, listener_id ORDER BY ms_played DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_3_prior,
        SIZE(COLLECT_SET(track_id) OVER w_device) AS distinct_track_id_in_device,
        SIZE(COLLECT_SET(listener_plan) OVER w_device) AS distinct_listener_plan_in_device,
        DENSE_RANK() OVER (PARTITION BY is_skipped, listener_plan ORDER BY ms_played DESC) AS ms_played_dense_rank,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY device), 2) AS ms_played_vs_device_avg,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY is_skipped, track_id ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        ROUND(AVG(ms_played) OVER (PARTITION BY device, listener_id ORDER BY ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_1_prior
    FROM with_details_listener
    WINDOW w_device AS (PARTITION BY device)
)
SELECT
    id,
    listener_id,
    ms_played,
    device,
    is_skipped,
    listener_interests,
    ms_played_share_of_device,
    prev_ms_played,
    ms_played_seq,
    ms_played_change,
    min_ms_played_so_far,
    distinct_source_in_device,
    ms_played_avg_3_prior,
    distinct_track_id_in_device,
    distinct_listener_plan_in_device,
    ms_played_dense_rank,
    ms_played_vs_device_avg,
    first_known_played_at
FROM with_windows
