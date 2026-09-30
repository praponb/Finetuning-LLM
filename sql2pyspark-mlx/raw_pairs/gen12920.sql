WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ADD_MONTHS(hire_date, 3) AS hire_date_plus_months,
        COALESCE(job_title, 'DIRECT') AS resolved_job_title
    FROM employees
    WHERE email LIKE '%@yahoo.com'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS dedup_rank
    FROM valid
    QUALIFY dedup_rank = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        is_remote,
        hire_date_plus_months,
        MAX(salary) OVER w_part AS last4_max_salary,
        AVG(salary) OVER w_part AS last4_avg_salary,
        COUNT(salary) OVER w_part AS last4_count_salary,
        NTILE(5) OVER (PARTITION BY DATE_TRUNC('YEAR', hire_date_plus_months), dept_id ORDER BY salary, id) AS salary_bucket
    FROM latest
    WINDOW w_part AS (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        full_name,
        email,
        dept_id,
        is_remote,
        last4_max_salary,
        last4_avg_salary,
        salary_bucket,
        ROW_NUMBER() OVER (PARTITION BY job_title, manager_id ORDER BY hire_date_plus_months DESC, id) AS event_seq,
        FIRST_VALUE(last4_max_salary, TRUE) OVER w_grp AS first_known_last4_max_salary,
        NTILE(3) OVER (PARTITION BY MONTH(hire_date_plus_months), job_title ORDER BY last4_count_salary DESC, id) AS last4_count_salary_bucket,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) AS salary_share,
        MAX(last4_max_salary) OVER w_recent AS manager_id_max_last4_max_salary
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY manager_id ORDER BY last4_avg_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY manager_id)
)
-- Join a dimension and aggregate
SELECT
    d.location,
    COUNT(*) AS cnt,
    MAX(s.event_seq) AS max_event_seq
FROM scored s
INNER JOIN departments d ON s.dept_id = d.id
GROUP BY d.location
ORDER BY location DESC NULLS LAST;
