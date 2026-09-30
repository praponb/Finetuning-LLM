WITH valid_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE signup_date >= DATE '2024-10-17'
      AND plan = 'family'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        signup_date,
        birth_year,
        interests,
        ROW_NUMBER() OVER (ORDER BY signup_date, id) AS signup_date_seq,
        ROW_NUMBER() OVER w_signup_date_month_by_birth_y AS seq_no,
        LAST_VALUE(birth_year) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_birth_year,
        ROUND(birth_year / NULLIF(SUM(birth_year) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date)), 0), 4) AS birth_year_share_of_signup_date_month,
        FIRST_VALUE(birth_year) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_birth_year,
        SUM(birth_year) OVER (ORDER BY signup_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS birth_year_sum_3,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY birth_year, id) AS birth_year_decile,
        COUNT(*) OVER w_signup_date_month_by_birth_y_3 AS rows_in_signup_date_month,
        LAST_VALUE(birth_year) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_birth_year_2
    FROM valid_listeners
    WINDOW w_signup_date_month_by_birth_y AS (PARTITION BY DATE_TRUNC('MONTH', signup_date) ORDER BY birth_year, id), w_signup_date_month_by_birth_y_3 AS (PARTITION BY DATE_TRUNC('MONTH', signup_date))
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(first_known_birth_year) OVER w_seq_no_signup_date_seq, 2) AS seq_no_signup_date_seq_avg_first_known_birth_yea,
        NTH_VALUE(signup_date, 2) OVER (PARTITION BY MONTH(signup_date), seq_no ORDER BY birth_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date,
        NTH_VALUE(last_birth_year, 2) OVER (PARTITION BY birth_year_decile, signup_date_seq ORDER BY signup_date_seq DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_birth_year,
        ROUND(birth_year / NULLIF(SUM(birth_year) OVER w_seq_no_birth_year_decile, 0), 4) AS birth_year_share_of_seq_no_birth_year_decile,
        ROW_NUMBER() OVER w_all_by_first_known_birth_yea AS seq_no_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY birth_year_decile, signup_date_seq ORDER BY signup_date_seq DESC), 3) AS signup_date_seq_percent_rank
    FROM with_windows
    WINDOW w_seq_no_signup_date_seq AS (PARTITION BY seq_no, signup_date_seq), w_seq_no_birth_year_decile AS (PARTITION BY seq_no, birth_year_decile), w_all_by_first_known_birth_yea AS (ORDER BY first_known_birth_year, id)
)
-- Final projection
SELECT
    id,
    signup_date,
    interests,
    signup_date_seq,
    last_birth_year,
    birth_year_share_of_signup_date_month,
    first_known_birth_year,
    birth_year_sum_3,
    birth_year_decile,
    rows_in_signup_date_month,
    last_birth_year_2,
    second_signup_date,
    second_last_birth_year,
    birth_year_share_of_seq_no_birth_year_decile,
    seq_no_2,
    signup_date_seq_percent_rank
FROM with_trends
ORDER BY last_birth_year DESC, id
LIMIT 10
