WITH base_plays AS (
    -- Filter plays
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
    WHERE device = 'car'
),
combined_details AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.is_skipped,
        s.source,
        l.birth_year AS listener_birth_year,
        l.interests AS listener_interests,
        l.plan AS listener_plan
    FROM base_plays s
    JOIN listeners l ON l.id = s.listener_id AND l.signup_date IS NOT NULL
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        FIRST_VALUE(listener_birth_year) IGNORE NULLS OVER (PARTITION BY YEAR(played_at), is_skipped ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_listener_birth_year,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id, listener_plan ORDER BY played_at ASC), 3) AS played_at_percent_rank,
        SIZE(COLLECT_SET(listener_plan) OVER w_source) AS distinct_listener_plan_in_source,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY YEAR(played_at), is_skipped ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY YEAR(played_at), is_skipped ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        COUNT(*) OVER (PARTITION BY source) AS rows_in_source,
        LAST_VALUE(played_at) IGNORE NULLS OVER (ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        ROUND(AVG(listener_birth_year) OVER (ORDER BY played_at ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS listener_birth_year_avg_2_prior,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY track_id, listener_plan ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        DENSE_RANK() OVER (ORDER BY played_at ASC, id) AS played_at_dense_rank,
        ROUND(listener_birth_year / NULLIF(SUM(listener_birth_year) OVER (PARTITION BY source), 0), 4) AS listener_birth_year_share_of_source,
        MAX(listener_birth_year) OVER (ORDER BY played_at ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS listener_birth_year_max_7,
        MIN(listener_birth_year) OVER (PARTITION BY source) AS source_min_listener_birth_year,
        DENSE_RANK() OVER w_all_by_played_at_ties AS played_at_dense_rank_2
    FROM combined_details
    WINDOW w_source AS (PARTITION BY source), w_all_by_played_at_ties AS (ORDER BY played_at ASC)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        played_at,
        ms_played,
        listener_birth_year,
        distinct_listener_plan_in_source,
        first_known_ms_played,
        second_played_at,
        rows_in_source,
        last_known_played_at,
        first_known_played_at,
        played_at_dense_rank,
        listener_birth_year_share_of_source,
        listener_birth_year_max_7,
        source_min_listener_birth_year,
        played_at_dense_rank_2,
        CASE WHEN listener_plan IN ('family', 'free') THEN 'group_a' WHEN listener_plan = 'student' THEN 'group_b' ELSE 'other' END AS listener_plan_group
    FROM ranked
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(first_known_played_at, 3) OVER (ORDER BY listener_birth_year_max_7, id) AS prev3_first_known_played_at,
        ROUND(PERCENT_RANK() OVER w_played_at_dense_rank_2_liste, 3) AS listener_birth_year_share_of_source_percent_rank,
        COUNT(*) OVER (PARTITION BY played_at_dense_rank, TO_DATE(played_at)) AS rows_in_played_at_dense_rank_played_at_to_date,
        NTILE(10) OVER (ORDER BY listener_birth_year_max_7, id) AS listener_birth_year_max_7_decile,
        ROUND(first_known_ms_played - AVG(first_known_ms_played) OVER (PARTITION BY played_at_dense_rank, TO_DATE(played_at)), 2) AS first_known_ms_played_vs_played_at_dense_rank_pl,
        LAST_VALUE(first_known_ms_played) IGNORE NULLS OVER (ORDER BY listener_birth_year_max_7, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_ms_played,
        RANK() OVER w_all_by_listener_birth_year_m AS listener_birth_year_max_7_rank,
        MIN(ms_played) OVER (PARTITION BY played_at_dense_rank_2, listener_plan_group ORDER BY listener_birth_year_share_of_source DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far
    FROM derived
    WINDOW w_played_at_dense_rank_2_liste AS (PARTITION BY played_at_dense_rank_2, listener_plan_group ORDER BY listener_birth_year_share_of_source DESC, id), w_all_by_listener_birth_year_m AS (ORDER BY listener_birth_year_max_7, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        listener_plan_group,
        DATE_TRUNC('MONTH', last_known_played_at) AS last_known_played_at_month,
        listener_birth_year_max_7_decile,
        COUNT(DISTINCT listener_plan_group) AS distinct_listener_plan_groups,
        COUNT(*) AS cnt,
        MIN(last_known_played_at) AS first_last_known_played_at
    FROM sequenced
    GROUP BY listener_plan_group, DATE_TRUNC('MONTH', last_known_played_at), listener_birth_year_max_7_decile
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        listener_plan_group,
        last_known_played_at_month,
        listener_birth_year_max_7_decile,
        distinct_listener_plan_groups,
        ROUND((UNIX_TIMESTAMP(first_last_known_played_at) - UNIX_TIMESTAMP(LAG(first_last_known_played_at) OVER (PARTITION BY listener_birth_year_max_7_decile ORDER BY distinct_listener_plan_groups, listener_plan_group, last_known_played_at_month, listener_birth_year_max_7_decile))) / 60, 1) AS minutes_since_prev_first_last_known_played_at,
        NTILE(4) OVER (PARTITION BY listener_birth_year_max_7_decile ORDER BY distinct_listener_plan_groups, listener_plan_group, last_known_played_at_month, listener_birth_year_max_7_decile) AS distinct_listener_plan_groups_quartile,
        ROW_NUMBER() OVER (PARTITION BY listener_birth_year_max_7_decile ORDER BY distinct_listener_plan_groups, listener_plan_group, last_known_played_at_month, listener_birth_year_max_7_decile) AS rn
    FROM grouped
)
SELECT
    listener_plan_group,
    last_known_played_at_month,
    minutes_since_prev_first_last_known_played_at,
    rn,
    CONCAT(listener_plan_group, '#', CAST(rn AS STRING)) AS listener_plan_group_ref
FROM windowed
ORDER BY last_known_played_at_month, rn
