WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(job_title, 1, 3) job_title_prefix,
        CONCAT_WS('/', level, full_name) level_full_name_key,
        COALESCE(level, 'N/A') level_clean
    FROM employees
    WHERE hire_date >= DATE '2023-08-29'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(salary) OVER (PARTITION BY manager_id, level ORDER BY hire_date, id) prev_salary,
        LEAD(salary) OVER (PARTITION BY manager_id, level ORDER BY hire_date, id) next_salary,
        ROW_NUMBER() OVER (PARTITION BY manager_id, level ORDER BY hire_date, id) seq_no,
        ROUND(salary / SUM(salary) OVER (PARTITION BY YEAR(hire_date)), 4) salary_share
    FROM valid
    WHERE job_title <> 'Director'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        next_salary,
        salary_share,
        MAX(salary_share) OVER w_running last4_max_salary_share,
        COUNT(salary_share) OVER w_running last4_count_salary_share,
        AVG(salary_share) OVER w_running last4_avg_salary_share,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('month', hire_date) ORDER BY seq_no, id) seq_no_bucket
    FROM enriched
    WINDOW w_running AS (PARTITION BY manager_id ORDER BY salary, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.email,
        a.manager_id,
        a.hire_date,
        a.is_remote,
        a.level,
        a.next_salary,
        a.salary_share,
        a.last4_max_salary_share,
        a.last4_count_salary_share,
        a.last4_avg_salary_share,
        p.level AS employee_level,
        p.full_name AS employee_full_name,
        p.hire_date AS employee_hire_date
    FROM metrics a
    LEFT JOIN employees p
        ON a.manager_id = p.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(next_salary) OVER (PARTITION BY level, employee_level ORDER BY last4_count_salary_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_next_salary,
        NTILE(3) OVER (ORDER BY last4_avg_salary_share DESC, id) last4_avg_salary_share_bucket,
        NTILE(4) OVER (ORDER BY last4_count_salary_share DESC, id) last4_count_salary_share_bucket
    FROM with_dims
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY employee_level ORDER BY last4_count_salary_share_bucket DESC) = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        employee_level,
        manager_id,
        level,
        running_next_salary
    FROM ranked
) src
PIVOT (
    AVG(running_next_salary) AS total,
    COUNT(1) AS cnt
    FOR level IN ('L3', 'L4')
);
