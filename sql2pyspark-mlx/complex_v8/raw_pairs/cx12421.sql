WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE HOUR(played_at) BETWEEN 8 AND 13
),
enriched_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        l.plan AS listener_plan
    FROM valid s
    JOIN listeners l ON l.id = s.listener_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY listener_plan, is_skipped)) AS distinct_track_id_in_listener_plan_is_skipped,
        COUNT(*) OVER (PARTITION BY listener_plan, is_skipped) AS rows_in_listener_plan_is_skipped,
        ROUND(AVG(ms_played) OVER (PARTITION BY device, listener_plan ORDER BY ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        LEAD(ms_played, 2) OVER w_is_skipped_listener_plan_by AS next2_ms_played,
        RANK() OVER (PARTITION BY device, listener_plan ORDER BY ms_played DESC, id) AS ms_played_rank,
        NTILE(3) OVER (PARTITION BY is_skipped, listener_plan ORDER BY ms_played, id) AS ms_played_tercile,
        ROUND(ms_played - AVG(ms_played) OVER w_is_skipped, 2) AS ms_played_vs_is_skipped_avg,
        SUM(ms_played) OVER (PARTITION BY device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_plan, is_skipped), 2) AS listener_plan_is_skipped_avg_ms_played,
        RANK() OVER w_all_by_ms_played_ties AS ms_played_rank_2,
        NTILE(5) OVER w_device_by_ms_played AS ms_played_quintile,
        SUM(ms_played) OVER (PARTITION BY device, listener_plan ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played_2,
        LAST_VALUE(ms_played) OVER (PARTITION BY device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        SUM(ms_played) OVER (PARTITION BY device, listener_plan ORDER BY ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS ms_played_sum_6_prior,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_plan, is_skipped), 2) AS ms_played_vs_listener_plan_is_skipped_avg
    FROM enriched_listener
    WINDOW w_is_skipped_listener_plan_by AS (PARTITION BY is_skipped, listener_plan ORDER BY ms_played, id), w_is_skipped AS (PARTITION BY is_skipped), w_all_by_ms_played_ties AS (ORDER BY ms_played DESC), w_device_by_ms_played AS (PARTITION BY device ORDER BY ms_played, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY ms_played_rank, track_id ORDER BY ms_played_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(ms_played_rank) OVER w_distinct_track_id_in_listene AS distinct_track_id_in_listener_plan_is_skipped_li,
        COUNT(*) OVER (PARTITION BY distinct_track_id_in_listener_plan_is_skipped, listener_id) AS rows_in_distinct_track_id_in_listener_plan_is_sk,
        MIN(ms_played_avg_2) OVER (PARTITION BY distinct_track_id_in_listener_plan_is_skipped, listener_id) AS distinct_track_id_in_listener_plan_is_skipped_li_2,
        LEAD(ms_played_rank, 2, 0) OVER (PARTITION BY ms_played_rank, track_id ORDER BY ms_played_rank, id) AS next2_ms_played_rank,
        SUM(running_ms_played) OVER (PARTITION BY ms_played_rank, track_id ORDER BY ms_played_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_ms_played_sum_4,
        LAST_VALUE(ms_played_tercile) OVER (PARTITION BY distinct_track_id_in_listener_plan_is_skipped, ms_played_tercile ORDER BY ms_played_avg_2 NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played_tercile,
        ROUND(CUME_DIST() OVER (ORDER BY ms_played_sum_6_prior), 3) AS ms_played_sum_6_prior_cume_dist
    FROM with_trends
    WINDOW w_distinct_track_id_in_listene AS (PARTITION BY distinct_track_id_in_listener_plan_is_skipped, listener_id)
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    is_skipped,
    listener_plan,
    distinct_track_id_in_listener_plan_is_skipped,
    ms_played_avg_2,
    ms_played_rank,
    ms_played_tercile,
    ms_played_vs_is_skipped_avg,
    listener_plan_is_skipped_avg_ms_played,
    ms_played_quintile,
    running_ms_played_2,
    ms_played_vs_listener_plan_is_skipped_avg,
    running_count,
    distinct_track_id_in_listener_plan_is_skipped_li,
    rows_in_distinct_track_id_in_listener_plan_is_sk,
    next2_ms_played_rank,
    last_ms_played_tercile,
    ms_played_sum_6_prior_cume_dist,
    UPPER(TRIM(listener_plan)) AS listener_plan_clean,
    CASE WHEN next2_ms_played_rank >= 7 THEN 'large' WHEN next2_ms_played_rank >= 4 THEN 'medium' ELSE 'small' END AS next2_ms_played_rank_tier
FROM with_windows
ORDER BY listener_plan, ms_played_sum_6_prior_cume_dist
