WITH prepared AS (
    -- Scope the listeners rows
    SELECT *
    FROM listeners
    WHERE plan = 'free'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(signup_date) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_signup_date,
        ROUND(CUME_DIST() OVER (ORDER BY signup_date, id), 3) AS signup_date_cume_dist,
        COUNT(*) OVER w_plan AS rows_in_plan,
        ROUND(birth_year / NULLIF(SUM(birth_year) OVER w_plan, 0), 4) AS birth_year_share_of_plan,
        NTILE(5) OVER (ORDER BY birth_year, id) AS birth_year_quintile,
        ROUND(birth_year - AVG(birth_year) OVER (PARTITION BY plan), 2) AS birth_year_vs_plan_avg,
        DENSE_RANK() OVER (ORDER BY signup_date NULLS LAST) AS signup_date_dense_rank,
        LAST_VALUE(birth_year) IGNORE NULLS OVER (ORDER BY signup_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_birth_year,
        LEAD(signup_date, 2) OVER w_all_by_signup_date_2 AS next2_signup_date,
        MAX(birth_year) OVER w_plan AS plan_max_birth_year,
        NTH_VALUE(signup_date, 2) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date
    FROM prepared
    WINDOW w_plan AS (PARTITION BY plan), w_all_by_signup_date_2 AS (ORDER BY signup_date NULLS LAST, id)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(plan) OVER (PARTITION BY signup_date_dense_rank, DATE_TRUNC('MONTH', signup_date))) AS distinct_plan_in_signup_date_dense_rank_signup_d,
        RANK() OVER w_all_by_signup_date_dense_ran AS signup_date_dense_rank_rank,
        NTH_VALUE(birth_year_quintile, 2) OVER (PARTITION BY signup_date_dense_rank ORDER BY rows_in_plan, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_birth_year_quintile,
        LEAD(signup_date_dense_rank, 2) OVER (PARTITION BY signup_date_dense_rank ORDER BY rows_in_plan, id) AS next2_signup_date_dense_rank,
        ROW_NUMBER() OVER w_signup_date_dense_rank_by_ro_2 AS rows_in_plan_seq,
        DATEDIFF(last_signup_date, LAG(last_signup_date) OVER (ORDER BY rows_in_plan, id)) AS days_since_prev_last_signup_date,
        NTH_VALUE(last_signup_date, 2) OVER (ORDER BY signup_date_dense_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_signup_date
    FROM sequenced
    WINDOW w_all_by_signup_date_dense_ran AS (ORDER BY signup_date_dense_rank DESC), w_signup_date_dense_rank_by_ro_2 AS (PARTITION BY signup_date_dense_rank ORDER BY rows_in_plan, id)
)
SELECT
    id,
    plan,
    birth_year,
    interests,
    last_signup_date,
    signup_date_cume_dist,
    rows_in_plan,
    birth_year_share_of_plan,
    birth_year_quintile,
    birth_year_vs_plan_avg,
    signup_date_dense_rank,
    second_signup_date,
    distinct_plan_in_signup_date_dense_rank_signup_d,
    signup_date_dense_rank_rank,
    second_birth_year_quintile,
    second_last_signup_date,
    FLOOR(rows_in_plan_seq / 2) * 2 AS rows_in_plan_seq_bucket,
    GREATEST(signup_date_dense_rank, rows_in_plan_seq) AS greatest_signup_date_dense_rank_rows_in_plan_seq
FROM with_windows
ORDER BY birth_year_share_of_plan DESC
