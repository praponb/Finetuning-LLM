WITH scoped_listeners AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        interests,
        COALESCE(plan, 'UNKNOWN') AS plan_or_unknown,
        LENGTH(TRIM(plan)) AS plan_len
    FROM listeners
    WHERE birth_year >= 1975
      AND plan IN ('family', 'student', 'free')
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(plan_len - 10) AS plan_len_distance,
        DATE_TRUNC('WEEK', signup_date) AS signup_date_week
    FROM scoped_listeners
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        username,
        plan,
        signup_date,
        plan_or_unknown,
        plan_len_distance,
        signup_date_week,
        RANK() OVER w_all_by_signup_date AS signup_date_rank,
        MAX(plan_len) OVER (PARTITION BY plan_or_unknown) AS plan_or_unknown_max_plan_len,
        ROW_NUMBER() OVER (ORDER BY signup_date_week, id) AS signup_date_week_seq,
        SUM(plan_len_distance) OVER (ORDER BY signup_date_week, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS plan_len_distance_sum_1_prior
    FROM labeled
    WINDOW w_all_by_signup_date AS (ORDER BY signup_date, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM with_windows
    WHERE signup_date_rank = 1
      AND plan_or_unknown IN ('free', 'student')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(signup_date_week_seq) OVER (ORDER BY signup_date_week_seq DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_signup_date_week_seq_so_far,
        ROUND(CUME_DIST() OVER (ORDER BY signup_date_week_seq DESC), 3) AS signup_date_week_seq_cume_dist,
        RANK() OVER (ORDER BY signup_date_week_seq DESC, id) AS signup_date_week_seq_rank,
        ROUND(plan_len_distance / NULLIF(SUM(plan_len_distance) OVER (PARTITION BY plan), 0), 4) AS plan_len_distance_share_of_plan,
        FIRST_VALUE(signup_date_week_seq) OVER (ORDER BY signup_date_week_seq DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_signup_date_week_seq
    FROM top_rows
),
summary AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', signup_date) AS signup_date_month,
        COUNT(*) FILTER (WHERE plan IN ('free', 'premium', 'student')) AS n_plan_in_free_premium_student,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(plan)) AS plan_list
    FROM ranked
    WHERE plan_len_distance_share_of_plan <= 0.6
    GROUP BY DATE_TRUNC('MONTH', signup_date)
    HAVING COUNT(*) >= 2
)
SELECT signup_date_month, n_plan_in_free_premium_student, plan_list
FROM summary
WHERE n_plan_in_free_premium_student > 4
ORDER BY signup_date_month DESC NULLS FIRST
