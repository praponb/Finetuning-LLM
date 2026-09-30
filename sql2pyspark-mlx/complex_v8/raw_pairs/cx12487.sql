WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label,
        DATE_TRUNC('WEEK', played_at) AS played_at_week
    FROM plays
),
joined AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.played_at_label,
        s.played_at_week,
        l.plan AS listener_plan,
        l.country AS listener_country
    FROM base s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.plan IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY track_id, is_skipped) AS rows_in_track_id_is_skipped,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id ORDER BY played_at_week DESC), 3) AS played_at_week_percent_rank,
        MAX(ms_played) OVER (PARTITION BY track_id, listener_id) AS track_id_listener_id_max_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY track_id, listener_id), 0), 4) AS ms_played_share_of_track_id_listener_id,
        LAG(ms_played, 2) OVER (PARTITION BY device ORDER BY ms_played ASC, id) AS prev2_ms_played,
        DENSE_RANK() OVER (PARTITION BY device, listener_country ORDER BY played_at_week DESC, id) AS played_at_week_dense_rank,
        NTH_VALUE(played_at_week, 2) OVER (PARTITION BY device, listener_country ORDER BY played_at_week DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_week,
        ms_played - LAG(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played, id) AS ms_played_change,
        SIZE(COLLECT_SET(listener_plan) OVER (PARTITION BY track_id, listener_id)) AS distinct_listener_plan_in_track_id_listener_id,
        NTILE(5) OVER (ORDER BY played_at_week, id) AS played_at_week_quintile,
        LAG(ms_played, 3) OVER (PARTITION BY track_id ORDER BY played_at_week DESC, id) AS prev3_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id, is_skipped), 2) AS track_id_is_skipped_avg_ms_played,
        ROUND(AVG(ms_played) OVER (ORDER BY played_at_week, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_6_prior,
        SUM(ms_played) OVER (PARTITION BY device ORDER BY ms_played ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_sum_7
    FROM joined
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        device,
        played_at_week,
        listener_plan,
        rows_in_track_id_is_skipped,
        track_id_listener_id_max_ms_played,
        ms_played_share_of_track_id_listener_id,
        prev2_ms_played,
        played_at_week_dense_rank,
        ms_played_change,
        distinct_listener_plan_in_track_id_listener_id,
        played_at_week_quintile,
        prev3_ms_played,
        track_id_is_skipped_avg_ms_played,
        ms_played_avg_6_prior,
        ms_played_sum_7,
        LAG(rows_in_track_id_is_skipped, 1, 0) OVER (PARTITION BY played_at_week_quintile ORDER BY ms_played DESC, id) AS prev_rows_in_track_id_is_skipped,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', second_played_at_week)) AS rows_in_second_played_at_week_month
    FROM windowed
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    listener_plan,
    rows_in_track_id_is_skipped,
    track_id_listener_id_max_ms_played,
    prev2_ms_played,
    ms_played_change,
    distinct_listener_plan_in_track_id_listener_id,
    played_at_week_quintile,
    prev3_ms_played,
    track_id_is_skipped_avg_ms_played,
    ms_played_sum_7,
    rows_in_second_played_at_week_month,
    GREATEST(prev3_ms_played, played_at_week_quintile) AS greatest_prev3_ms_played_played_at_week_quintile,
    CONCAT(listener_plan, '#', CAST(track_id AS STRING)) AS listener_plan_ref
FROM metrics
WHERE ms_played_sum_7 >= 244000
ORDER BY ms_played_sum_7, track_id DESC
