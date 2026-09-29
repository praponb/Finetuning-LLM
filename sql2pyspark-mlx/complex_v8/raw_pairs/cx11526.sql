WITH filtered_listeners AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        signup_date,
        interests,
        DATE_ADD(signup_date, 7) AS signup_date_plus_7d
    FROM listeners
    WHERE plan NOT IN ('student', 'family')
      AND LOWER(username) LIKE '%fa%'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        plan,
        signup_date,
        interests,
        signup_date_plus_7d,
        DATEDIFF(signup_date_plus_7d, LAG(signup_date_plus_7d) OVER (ORDER BY signup_date, id)) AS days_since_prev_signup_date_plus_7d,
        RANK() OVER (ORDER BY signup_date DESC) AS signup_date_rank,
        NTH_VALUE(signup_date, 2) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date,
        ROUND(CUME_DIST() OVER w_all_by_signup_date, 3) AS signup_date_cume_dist,
        NTH_VALUE(signup_date_plus_7d, 2) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date_plus_7d,
        DENSE_RANK() OVER w_plan_by_signup_date_ties AS signup_date_dense_rank,
        LAG(signup_date_plus_7d) OVER w_all_by_signup_date_plus_7d AS prev_signup_date_plus_7d,
        ROW_NUMBER() OVER w_all_by_signup_date_plus_7d AS seq_no,
        DENSE_RANK() OVER (ORDER BY signup_date) AS signup_date_dense_rank_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY signup_date_plus_7d ASC, id), 3) AS signup_date_plus_7d_percent_rank,
        DENSE_RANK() OVER (ORDER BY signup_date DESC, id) AS signup_date_dense_rank_3
    FROM filtered_listeners
    WINDOW w_all_by_signup_date AS (ORDER BY signup_date, id), w_plan_by_signup_date_ties AS (PARTITION BY plan ORDER BY signup_date NULLS LAST), w_all_by_signup_date_plus_7d AS (ORDER BY signup_date_plus_7d, id)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        plan,
        signup_date,
        interests,
        signup_date_plus_7d,
        days_since_prev_signup_date_plus_7d,
        signup_date_rank,
        second_signup_date,
        signup_date_cume_dist,
        second_signup_date_plus_7d,
        signup_date_dense_rank,
        prev_signup_date_plus_7d,
        signup_date_dense_rank_2,
        signup_date_plus_7d_percent_rank,
        signup_date_dense_rank_3
    FROM with_windows
    WHERE seq_no <= 3
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY signup_date) AS signup_date_dense_rank_4,
        LEAD(signup_date_plus_7d, 3) OVER w_all_by_second_signup_date_pl AS next3_signup_date_plus_7d,
        ROUND(signup_date_cume_dist - AVG(signup_date_cume_dist) OVER w_signup_date_rank, 2) AS signup_date_cume_dist_vs_signup_date_rank_avg,
        MAX(signup_date_dense_rank_3) OVER (PARTITION BY signup_date_dense_rank_3, signup_date_dense_rank_2) AS signup_date_dense_rank_3_signup_date_dense_rank_,
        MAX(signup_date_cume_dist) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_signup_date_cume_dist_so_far
    FROM top_per_group
    WINDOW w_all_by_second_signup_date_pl AS (ORDER BY second_signup_date_plus_7d, id), w_signup_date_rank AS (PARTITION BY signup_date_rank)
)
-- Final projection
SELECT
    id,
    signup_date,
    interests,
    signup_date_plus_7d,
    days_since_prev_signup_date_plus_7d,
    signup_date_rank,
    second_signup_date,
    signup_date_cume_dist,
    second_signup_date_plus_7d,
    signup_date_dense_rank,
    signup_date_dense_rank_2,
    signup_date_plus_7d_percent_rank,
    signup_date_dense_rank_3,
    signup_date_cume_dist_vs_signup_date_rank_avg,
    signup_date_dense_rank_3_signup_date_dense_rank_,
    max_signup_date_cume_dist_so_far,
    ROUND(signup_date_rank / NULLIF(signup_date_dense_rank_4, 0), 3) AS signup_date_rank_per_signup_date_dense_rank_4
FROM with_trends
ORDER BY signup_date_dense_rank
