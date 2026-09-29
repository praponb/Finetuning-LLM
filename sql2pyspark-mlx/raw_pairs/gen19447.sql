WITH base AS (
    SELECT
        id,
        manager_id,
        salary,
        hire_date,
        level,
        CAST(is_remote AS INT) AS is_remote_flag,
        UPPER(full_name) AS full_name_upper,
        COALESCE(level, 'DIRECT') AS level_filled
    FROM employees
    WHERE dept_id IN (16, 40)
),
scored AS (
    SELECT
        id,
        salary,
        is_remote_flag,
        full_name_upper,
        level_filled,
        LEAD(is_remote_flag) OVER w_recent AS next_is_remote_flag,
        LEAD(salary) OVER w_recent AS next_salary,
        LAG(salary) OVER w_recent AS prev_salary,
        ROW_NUMBER() OVER w_recent AS row_seq,
        SUM(salary) OVER w_hist AS cumulative_salary
    FROM base
    WHERE level IN ('L4', 'L1', 'L5')
    WINDOW w_recent AS (PARTITION BY manager_id ORDER BY salary, id), w_hist AS (PARTITION BY level ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        *,
        LEAD(next_is_remote_flag) OVER (ORDER BY salary, id) AS next_next_is_remote_flag,
        LAG(next_is_remote_flag) OVER (ORDER BY salary, id) AS prev_next_is_remote_flag,
        LEAD(prev_salary) OVER (ORDER BY salary, id) AS next_prev_salary,
        ROW_NUMBER() OVER (ORDER BY salary, id) AS seq_no
    FROM scored
),
with_windows_employees AS (
    SELECT
        id,
        salary,
        is_remote_flag,
        full_name_upper,
        next_is_remote_flag,
        prev_salary,
        prev_next_is_remote_flag,
        LEAD(row_seq) OVER w_running AS next_row_seq,
        LAG(next_is_remote_flag) OVER w_running AS prev_next_is_remote_flag_2,
        ROW_NUMBER() OVER w_running AS event_seq,
        AVG(next_prev_salary) OVER (ORDER BY next_is_remote_flag, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS next_prev_salary_avg_3_prior
    FROM with_windows
    WINDOW w_running AS (ORDER BY prev_salary, id)
),
metrics AS (
    SELECT
        id,
        is_remote_flag,
        next_is_remote_flag,
        prev_salary,
        prev_next_is_remote_flag,
        prev_next_is_remote_flag_2,
        event_seq,
        SUM(prev_salary) OVER (ORDER BY next_row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_salary,
        SUM(is_remote_flag) OVER (ORDER BY next_prev_salary_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_remote_flag,
        ROW_NUMBER() OVER (ORDER BY prev_salary DESC, id) AS event_seq_2
    FROM with_windows_employees
),
base_employees AS (
    SELECT
        id,
        is_remote_flag,
        next_is_remote_flag,
        prev_salary,
        prev_next_is_remote_flag,
        prev_next_is_remote_flag_2,
        event_seq,
        cumulative_prev_salary,
        cumulative_is_remote_flag,
        event_seq_2,
        FLOOR(is_remote_flag) AS is_remote_flag_whole,
        cumulative_is_remote_flag - next_is_remote_flag AS cumulative_is_remote_flag_minus_next_is_remote_flag
    FROM metrics
    WHERE next_is_remote_flag <= 70
)
SELECT
    *,
    DENSE_RANK() OVER w_grp AS event_seq_2_dense_rank,
    NTILE(4) OVER (ORDER BY cumulative_is_remote_flag_minus_next_is_remote_flag DESC, id) AS cumulative_is_remote_flag_minus_next_is_remote_flag_bucket,
    NTILE(4) OVER (ORDER BY prev_next_is_remote_flag_2 DESC, id) AS prev_next_is_remote_flag_2_bucket,
    LEAD(event_seq, 1) OVER (ORDER BY cumulative_is_remote_flag, id) AS next_event_seq,
    LEAD(event_seq_2, 1) OVER w_ordered AS next_event_seq_2
FROM base_employees
WINDOW w_grp AS (ORDER BY event_seq_2 NULLS LAST), w_ordered AS (ORDER BY prev_salary, id)
