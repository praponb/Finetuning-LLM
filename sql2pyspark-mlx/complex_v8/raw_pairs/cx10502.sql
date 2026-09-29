WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE track_id IS NOT NULL
      AND is_skipped = TRUE
),
combined_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        l.plan AS listener_plan,
        l.username AS listener_username
    FROM scoped_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY device), 0), 4) AS ms_played_share_of_device,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS listener_id_avg_ms_played,
        SIZE(COLLECT_SET(listener_plan) OVER (PARTITION BY listener_id)) AS distinct_listener_plan_in_listener_id,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY track_id, listener_id)) AS distinct_source_in_track_id_listener_id,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_id, listener_id), 0), 4) AS ms_played_share_of_track_id_listener_id,
        LAG(ms_played, 2) OVER (PARTITION BY listener_plan, track_id ORDER BY ms_played DESC, id) AS prev2_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER w_track_id_listener_id, 2) AS ms_played_vs_track_id_listener_id_avg,
        SUM(ms_played) OVER (PARTITION BY listener_id) AS listener_id_sum_ms_played,
        LAG(ms_played) OVER w_listener_id_by_ms_played AS prev_ms_played
    FROM combined_details
    WINDOW w_track_id_listener_id AS (PARTITION BY track_id, listener_id), w_listener_id_by_ms_played AS (PARTITION BY listener_id ORDER BY ms_played DESC, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(ms_played_avg_2_prior) OVER (ORDER BY listener_id_sum_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played_avg_2_prior,
        MIN(prev_ms_played) OVER (PARTITION BY device) AS device_min_prev_ms_played,
        LEAD(listener_id_sum_ms_played, 2) OVER w_listener_plan_by_distinct_so AS next2_listener_id_sum_ms_played,
        MAX(ms_played) OVER (PARTITION BY device) AS device_max_ms_played,
        MIN(ms_played_share_of_device) OVER w_device AS device_min_ms_played_share_of_device
    FROM windowed
    WINDOW w_listener_plan_by_distinct_so AS (PARTITION BY listener_plan ORDER BY distinct_source_in_track_id_listener_id NULLS LAST, id), w_device AS (PARTITION BY device)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        listener_id,
        COUNT(*) AS row_count,
        SUM(ms_played_share_of_device) AS total_ms_played_share_of_device
    FROM metrics
    GROUP BY listener_id
)
SELECT
    listener_id,
    row_count,
    total_ms_played_share_of_device,
    CASE WHEN row_count >= 9 THEN 'HIGH' WHEN row_count >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS row_count_tier
FROM stats
WHERE listener_id IS NOT NULL
ORDER BY total_ms_played_share_of_device, listener_id
LIMIT 10
