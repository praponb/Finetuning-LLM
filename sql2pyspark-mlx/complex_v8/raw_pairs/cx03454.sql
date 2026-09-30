WITH valid_plays AS (
    -- Filter plays
    SELECT *
    FROM plays
    WHERE is_skipped = TRUE
),
with_parent AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        l.signup_date AS listener_signup_date,
        l.plan AS listener_plan
    FROM valid_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id AND l.country <> 'JP'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(listener_signup_date) IGNORE NULLS OVER (PARTITION BY YEAR(listener_signup_date) ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_listener_signup_date,
        MAX(ms_played) OVER w_listener_signup_date_year AS listener_signup_date_year_max_ms_played,
        MAX(ms_played) OVER (PARTITION BY YEAR(listener_signup_date) ORDER BY played_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_max_3,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY YEAR(listener_signup_date)), 0), 4) AS ms_played_share_of_listener_signup_date_year,
        NTILE(3) OVER (PARTITION BY YEAR(listener_signup_date) ORDER BY played_at DESC, id) AS played_at_tercile,
        LAST_VALUE(listener_signup_date) OVER (PARTITION BY device, is_skipped ORDER BY listener_signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_listener_signup_date,
        DENSE_RANK() OVER (PARTITION BY device, is_skipped ORDER BY listener_signup_date DESC) AS listener_signup_date_dense_rank,
        COUNT(*) OVER w_listener_signup_date_year AS rows_in_listener_signup_date_year,
        ROW_NUMBER() OVER (PARTITION BY device, is_skipped ORDER BY listener_signup_date DESC, id) AS listener_signup_date_seq,
        LEAD(played_at) OVER (PARTITION BY device, is_skipped ORDER BY listener_signup_date DESC, id) AS next_played_at,
        SUM(ms_played) OVER (ORDER BY listener_signup_date ASC NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS ms_played_sum_4,
        ROW_NUMBER() OVER (ORDER BY listener_signup_date ASC NULLS LAST, id) AS seq_no,
        MIN(ms_played) OVER (PARTITION BY YEAR(listener_signup_date)) AS listener_signup_date_year_min_ms_played,
        SIZE(COLLECT_SET(track_id) OVER w_listener_signup_date_year) AS distinct_track_id_in_listener_signup_date_year
    FROM with_parent
    WINDOW w_listener_signup_date_year AS (PARTITION BY YEAR(listener_signup_date))
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        listener_signup_date,
        listener_plan,
        first_known_listener_signup_date,
        listener_signup_date_year_max_ms_played,
        ms_played_max_3,
        ms_played_share_of_listener_signup_date_year,
        played_at_tercile,
        last_listener_signup_date,
        listener_signup_date_dense_rank,
        rows_in_listener_signup_date_year,
        next_played_at,
        ms_played_sum_4,
        seq_no,
        listener_signup_date_year_min_ms_played,
        distinct_track_id_in_listener_signup_date_year
    FROM with_windows
    WHERE listener_signup_date_seq <= 3
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        track_id,
        played_at,
        is_skipped,
        listener_signup_date,
        listener_plan,
        first_known_listener_signup_date,
        listener_signup_date_year_max_ms_played,
        ms_played_share_of_listener_signup_date_year,
        listener_signup_date_dense_rank,
        rows_in_listener_signup_date_year,
        ms_played_sum_4,
        listener_signup_date_year_min_ms_played,
        distinct_track_id_in_listener_signup_date_year,
        ROW_NUMBER() OVER w_is_skipped_by_last_listener AS last_listener_signup_date_seq,
        SUM(ms_played_max_3) OVER (PARTITION BY seq_no) AS seq_no_sum_ms_played_max_3,
        ROW_NUMBER() OVER (ORDER BY ms_played NULLS LAST, id) AS ms_played_seq
    FROM top_per_group
    WINDOW w_is_skipped_by_last_listener AS (PARTITION BY is_skipped ORDER BY last_listener_signup_date ASC, id)
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_track_id_in_listener_signup_date_year,
        COUNT(*) AS cnt,
        COUNT(DISTINCT listener_plan) AS distinct_listener_plans,
        SUM(seq_no_sum_ms_played_max_3) AS total_seq_no_sum_ms_played_max_3,
        ARRAY_SORT(COLLECT_SET(listener_plan)) AS listener_plan_list,
        COUNT(DISTINCT track_id) AS distinct_track_ids
    FROM ranked
    WHERE last_listener_signup_date_seq > 2
    GROUP BY distinct_track_id_in_listener_signup_date_year
)
SELECT
    distinct_track_id_in_listener_signup_date_year,
    distinct_listener_plans,
    total_seq_no_sum_ms_played_max_3,
    listener_plan_list
FROM totals
ORDER BY distinct_track_id_in_listener_signup_date_year DESC
