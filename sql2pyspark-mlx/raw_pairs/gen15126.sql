WITH filtered AS (
    SELECT
        id,
        full_name,
        email,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        COALESCE(email, 'unassigned') AS email_filled
    FROM employees
    WHERE hire_date >= TO_DATE('2023-08-16')
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level, job_title ORDER BY salary DESC, id ASC) AS row_num
        FROM filtered
    ) x
    WHERE row_num = 1
),
scored AS (
    SELECT
        id,
        full_name,
        email,
        hire_date,
        level,
        email_filled,
        row_num,
        ROW_NUMBER() OVER (PARTITION BY level, job_title ORDER BY salary ASC, id ASC) AS row_seq,
        NTILE(5) OVER (PARTITION BY MONTH(hire_date), level ORDER BY salary DESC, id ASC) AS salary_bucket,
        SUM(salary) OVER (PARTITION BY job_title) AS job_title_sum_salary
    FROM ranked
    WHERE job_title IN ('Director', 'Designer', 'Engineer')
),
enriched AS (
    SELECT
        id,
        hire_date,
        email_filled,
        row_seq,
        salary_bucket,
        AVG(job_title_sum_salary) OVER (PARTITION BY level ORDER BY row_num ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS job_title_sum_salary_avg_3,
        ROW_NUMBER() OVER (ORDER BY salary_bucket ASC, id ASC) AS seq_no,
        ROUND(job_title_sum_salary / SUM(job_title_sum_salary) OVER (PARTITION BY DATE_TRUNC('MONTH', hire_date)), 4) AS job_title_sum_salary_share,
        AVG(salary_bucket) OVER (PARTITION BY level) AS level_avg_salary_bucket,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id ASC) AS event_seq
    FROM scored
),
with_windows AS (
    SELECT
        *,
        SUM(job_title_sum_salary_share) OVER w_grp AS running_sum_job_title_sum_salary_share,
        MIN(job_title_sum_salary_share) OVER w_grp AS running_min_job_title_sum_salary_share,
        MAX(job_title_sum_salary_share) OVER w_grp AS running_max_job_title_sum_salary_share,
        COUNT(job_title_sum_salary_share) OVER w_grp AS running_count_job_title_sum_salary_share
    FROM enriched
    WINDOW w_grp AS (ORDER BY level_avg_salary_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_employees AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY running_max_job_title_sum_salary_share ASC, id ASC) AS row_seq_2,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY MONTH(hire_date)), 4) AS seq_no_share,
        SUM(job_title_sum_salary_share) OVER (ORDER BY running_sum_job_title_sum_salary_share ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_job_title_sum_salary_share
    FROM with_windows
)
SELECT
    id,
    metric,
    value
FROM (
    SELECT
        id,
        running_max_job_title_sum_salary_share,
        running_sum_job_title_sum_salary_share,
        job_title_sum_salary_share,
        seq_no_share
    FROM with_windows_employees
) src
UNPIVOT (
    value FOR metric IN (running_max_job_title_sum_salary_share, running_sum_job_title_sum_salary_share, job_title_sum_salary_share, seq_no_share)
)
