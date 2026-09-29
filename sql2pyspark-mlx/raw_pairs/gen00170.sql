WITH enriched AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        FIRST_VALUE(job_title, TRUE) OVER (PARTITION BY dept_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title,
        AVG(salary) OVER w_part AS salary_avg_2,
        MAX(salary) OVER (PARTITION BY dept_id) AS dept_id_max_salary,
        FIRST_VALUE(job_title, TRUE) OVER (PARTITION BY dept_id, level ORDER BY hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_job_title_2
    FROM employees
    WINDOW w_part AS (PARTITION BY job_title ORDER BY salary ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
enriched_employees AS (
    SELECT
        id,
        job_title,
        hire_date,
        DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary ASC) AS salary_dense_rank,
        NTILE(4) OVER (PARTITION BY job_title ORDER BY salary_avg_2 DESC, id ASC) AS salary_avg_2_bucket
    FROM enriched
),
with_windows AS (
    SELECT
        id,
        job_title,
        salary_avg_2_bucket,
        LEAD(salary_avg_2_bucket) OVER w_hist AS next_salary_avg_2_bucket,
        LAG(salary_avg_2_bucket) OVER w_hist AS prev_salary_avg_2_bucket,
        LAG(salary_dense_rank) OVER w_hist AS prev_salary_dense_rank,
        LEAD(salary_dense_rank) OVER w_hist AS next_salary_dense_rank,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM enriched_employees
    WINDOW w_hist AS (PARTITION BY job_title ORDER BY salary_dense_rank ASC, id ASC)
),
deduped AS (
    SELECT
        id,
        salary_avg_2_bucket,
        next_salary_avg_2_bucket,
        prev_salary_avg_2_bucket,
        next_salary_dense_rank,
        row_seq
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY next_salary_avg_2_bucket DESC, id ASC) AS rank_in_group
        FROM with_windows
    ) x
    WHERE rank_in_group = 1
)
SELECT
    id,
    next_salary_avg_2_bucket,
    row_seq,
    ROW_NUMBER() OVER (ORDER BY prev_salary_avg_2_bucket ASC, id ASC) AS event_seq,
    SUM(salary_avg_2_bucket) OVER (ORDER BY prev_salary_avg_2_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_avg_2_bucket
FROM deduped;
