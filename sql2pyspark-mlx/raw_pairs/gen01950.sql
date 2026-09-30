WITH cleaned AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        IF(is_remote, 'Yes', 'No') is_remote_label,
        LENGTH(full_name) full_name_length
    FROM employees
    WHERE hire_date > DATE '2023-08-03'
      AND job_title IS NOT NULL
),
kept_keys AS (
    SELECT dept_id
    FROM cleaned
    INTERSECT ALL
    SELECT dept_id
    FROM employees
    WHERE job_title IS NOT NULL
),
kept_rows AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.dept_id = k.dept_id
),
cleaned_employees AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        is_remote_label,
        full_name_length,
        LAST_DAY(hire_date) hire_date_month_end
    FROM kept_rows
    WHERE job_title IS NOT NULL
),
with_windows AS (
    SELECT
        id,
        full_name,
        email,
        job_title,
        salary,
        hire_date,
        is_remote,
        is_remote_label,
        full_name_length,
        RANK() OVER (PARTITION BY job_title ORDER BY full_name_length NULLS LAST) full_name_length_rank,
        STDDEV_SAMP(salary) OVER (PARTITION BY job_title, dept_id) job_title_stddev_samp_salary,
        RANK() OVER (ORDER BY salary) salary_rank
    FROM cleaned_employees
),
valid AS (
    SELECT
        *,
        GREATEST(full_name_length_rank, full_name_length) greatest_full_name_length_rank_full_name_length,
        REPLACE(full_name, ' ', '-') full_name_slug,
        COALESCE(job_title, 'N/A') resolved_job_title
    FROM with_windows
    WHERE job_title IS NOT NULL
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY job_title ORDER BY full_name_length DESC NULLS FIRST) full_name_length_cume_dist,
        STDDEV_SAMP(greatest_full_name_length_rank_full_name_length) OVER (PARTITION BY job_title) job_title_stddev_samp_greatest_full_name_length_rank_full_name_length,
        ROUND(job_title_stddev_samp_salary / SUM(job_title_stddev_samp_salary) OVER (PARTITION BY job_title), 4) job_title_stddev_samp_salary_share
    FROM valid
),
totals AS (
    SELECT
        CASE WHEN job_title_stddev_samp_greatest_full_name_length_rank_full_name_length >= 81 THEN 'PREMIUM' ELSE 'STANDARD' END AS job_title_stddev_samp_greatest_full_name_length_rank_full_name_length_tier,
        DATE_TRUNC('MONTH', hire_date) AS activity_month,
        COUNT(*) FILTER (WHERE job_title = 'Engineer') AS engineer_rows,
        SIZE(COLLECT_SET(job_title)) AS num_job_titles,
        COUNT(DISTINCT job_title) AS distinct_job_titles
    FROM metrics
    GROUP BY ROLLUP (CASE WHEN job_title_stddev_samp_greatest_full_name_length_rank_full_name_length >= 81 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', hire_date))
    HAVING COUNT(*) FILTER (WHERE job_title = 'Engineer') > 1
)
SELECT
    job_title_stddev_samp_greatest_full_name_length_rank_full_name_length_tier,
    activity_month,
    distinct_job_titles
FROM totals
ORDER BY distinct_job_titles DESC NULLS FIRST;
