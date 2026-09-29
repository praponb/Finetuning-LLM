WITH enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        LEAD(salary, 1) OVER (PARTITION BY manager_id ORDER BY hire_date ASC, id ASC) AS next_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title, dept_id), 4) AS salary_share
    FROM employees
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(salary_share, 1) OVER w_running AS next_salary_share,
        ROW_NUMBER() OVER (ORDER BY hire_date DESC, id ASC) AS row_seq
    FROM enriched
    WINDOW w_running AS (PARTITION BY level ORDER BY next_salary ASC, id ASC)
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(next_salary_share, 1) OVER (PARTITION BY level ORDER BY row_seq ASC, id ASC) AS prev_next_salary_share,
        LAST_VALUE(job_title, TRUE) OVER (PARTITION BY manager_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title
    FROM scored
)
-- Grouped metrics
SELECT
    CASE WHEN prev_next_salary_share >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_next_salary_share_tier,
    job_title,
    MAX(salary_share) FILTER (WHERE level = 'L1') AS l1_max_salary_share,
    SUM(salary_share) AS total_salary_share,
    PERCENTILE_APPROX(salary_share, ARRAY(0.25, 0.5, 0.75)) AS salary_share_quartiles,
    SUM(CASE WHEN job_title = 'Designer' THEN salary ELSE 0 END) AS designer_salary,
    PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles
FROM scored_employees
GROUP BY 1, 2
HAVING MAX(salary_share) FILTER (WHERE level = 'L1') >= 0
ORDER BY job_title ASC NULLS LAST
