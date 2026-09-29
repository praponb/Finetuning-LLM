WITH recent_listeners AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        QUARTER(signup_date) AS signup_date_quarter
    FROM listeners
    WHERE SIZE(interests) >= 2
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(signup_date, 2) OVER (ORDER BY birth_year ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date,
        SUM(signup_date_quarter) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY birth_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_signup_date_quarter,
        LAST_VALUE(signup_date_quarter) IGNORE NULLS OVER (ORDER BY birth_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_signup_date_quarter,
        ROW_NUMBER() OVER (ORDER BY birth_year DESC, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY signup_date_quarter) AS rows_in_signup_date_quarter,
        MAX(signup_date_quarter) OVER w_signup_date_quarter AS signup_date_quarter_max_signup_date_quarter,
        MIN(birth_year) OVER (PARTITION BY plan, signup_date_quarter) AS plan_signup_date_quarter_min_birth_year,
        ROUND(birth_year - AVG(birth_year) OVER w_signup_date_quarter, 2) AS birth_year_vs_signup_date_quarter_avg,
        COUNT(*) OVER (ORDER BY birth_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROW_NUMBER() OVER (PARTITION BY plan, country ORDER BY signup_date_quarter, id) AS rn
    FROM recent_listeners
    WINDOW w_signup_date_quarter AS (PARTITION BY signup_date_quarter)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE rn = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        running_signup_date_quarter,
        last_known_signup_date_quarter,
        seq_no,
        rows_in_signup_date_quarter,
        signup_date_quarter_max_signup_date_quarter,
        plan_signup_date_quarter_min_birth_year,
        rn,
        birth_year - LAG(birth_year) OVER w_seq_no_by_birth_year_vs_sign AS birth_year_change,
        COUNT(*) OVER (PARTITION BY signup_date_quarter, YEAR(signup_date)) AS rows_in_signup_date_quarter_signup_date_year,
        SIZE(COLLECT_SET(plan) OVER (PARTITION BY last_known_signup_date_quarter, signup_date_quarter)) AS distinct_plan_in_last_known_signup_date_quarter_,
        ROUND(PERCENT_RANK() OVER (ORDER BY signup_date_quarter_max_signup_date_quarter DESC, id), 3) AS signup_date_quarter_max_signup_date_quarter_perc
    FROM first_rows
    WINDOW w_seq_no_by_birth_year_vs_sign AS (PARTITION BY seq_no ORDER BY birth_year_vs_signup_date_quarter_avg, id)
)
-- Final projection
SELECT
    id,
    username,
    country,
    signup_date,
    birth_year,
    running_signup_date_quarter,
    last_known_signup_date_quarter,
    seq_no,
    rows_in_signup_date_quarter,
    signup_date_quarter_max_signup_date_quarter,
    plan_signup_date_quarter_min_birth_year,
    rn,
    rows_in_signup_date_quarter_signup_date_year,
    distinct_plan_in_last_known_signup_date_quarter_,
    COALESCE(birth_year_change, 0) AS birth_year_change_filled
FROM ranked
ORDER BY seq_no NULLS LAST, birth_year_change_filled
