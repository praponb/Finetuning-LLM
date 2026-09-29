WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM-dd') AS played_at_label
    FROM plays
    WHERE listener_id >= 14
      AND ms_played > 112786
),
combined_details AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.source,
        s.played_at_label,
        l.plan AS listener_plan,
        l.signup_date AS listener_signup_date
    FROM prepared_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        source,
        played_at_label,
        listener_plan,
        listener_signup_date,
        MAX(ms_played) OVER (ORDER BY ms_played ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_max_7,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_plan ORDER BY listener_signup_date DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_4,
        ROUND(AVG(ms_played) OVER w_listener_plan, 2) AS listener_plan_avg_ms_played,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id, listener_plan ORDER BY listener_signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        COUNT(*) OVER (PARTITION BY listener_id, listener_plan ORDER BY listener_signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LEAD(listener_signup_date, 2) OVER w_listener_plan_by_listener_si_2 AS next2_listener_signup_date,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_plan ORDER BY listener_signup_date DESC), 3) AS listener_signup_date_cume_dist,
        RANK() OVER (PARTITION BY listener_plan ORDER BY listener_signup_date DESC, id) AS listener_signup_date_rank,
        NTILE(3) OVER w_listener_id_listener_plan_by_2 AS listener_signup_date_tercile,
        SIZE(COLLECT_SET(listener_id) OVER w_listener_plan) AS distinct_listener_id_in_listener_plan,
        LEAD(ms_played) OVER (PARTITION BY listener_plan ORDER BY listener_signup_date DESC, id) AS next_ms_played,
        SUM(ms_played) OVER w_listener_plan AS listener_plan_sum_ms_played,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id, listener_plan ORDER BY listener_signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY listener_plan), 0), 4) AS ms_played_share_of_listener_plan
    FROM combined_details
    WINDOW w_listener_plan AS (PARTITION BY listener_plan), w_listener_plan_by_listener_si_2 AS (PARTITION BY listener_plan ORDER BY listener_signup_date DESC, id), w_listener_id_listener_plan_by_2 AS (PARTITION BY listener_id, listener_plan ORDER BY listener_signup_date, id)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        track_id,
        source,
        played_at_label,
        listener_plan,
        listener_signup_date,
        ms_played_max_7,
        ms_played_avg_4,
        listener_plan_avg_ms_played,
        last_known_ms_played,
        running_count,
        next2_listener_signup_date,
        listener_signup_date_cume_dist,
        listener_signup_date_tercile,
        distinct_listener_id_in_listener_plan,
        next_ms_played,
        listener_plan_sum_ms_played,
        first_known_ms_played,
        ms_played_share_of_listener_plan
    FROM ranked
    WHERE listener_signup_date_rank = 1
      AND ms_played_avg_4 < 301250.0
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        source,
        played_at_label,
        listener_plan,
        ms_played_max_7,
        ms_played_avg_4,
        listener_plan_avg_ms_played,
        next2_listener_signup_date,
        listener_signup_date_cume_dist,
        listener_signup_date_tercile,
        distinct_listener_id_in_listener_plan,
        listener_plan_sum_ms_played,
        first_known_ms_played,
        ms_played_share_of_listener_plan,
        LEAD(next2_listener_signup_date, 3) OVER (PARTITION BY listener_signup_date_tercile ORDER BY distinct_listener_id_in_listener_plan, id) AS next3_next2_listener_signup_date,
        ROUND(AVG(listener_plan_sum_ms_played) OVER (PARTITION BY MONTH(listener_signup_date)), 2) AS listener_signup_date_month_avg_listener_plan_sum,
        MAX(listener_signup_date_tercile) OVER (PARTITION BY MONTH(listener_signup_date)) AS listener_signup_date_month_max_listener_signup_d,
        MAX(running_count) OVER (ORDER BY distinct_listener_id_in_listener_plan, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS running_count_max_4
    FROM first_rows
)
SELECT
    id,
    played_at_label,
    listener_plan,
    ms_played_avg_4,
    listener_plan_avg_ms_played,
    next2_listener_signup_date,
    distinct_listener_id_in_listener_plan,
    listener_plan_sum_ms_played,
    ms_played_share_of_listener_plan,
    next3_next2_listener_signup_date,
    listener_signup_date_month_avg_listener_plan_sum,
    running_count_max_4,
    GREATEST(running_count_max_4, ms_played_avg_4) AS greatest_running_count_max_4_ms_played_avg_4
FROM with_trends
ORDER BY running_count_max_4 DESC, distinct_listener_id_in_listener_plan NULLS LAST
