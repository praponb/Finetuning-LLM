WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        salary,
        is_remote,
        level,
        CAST(is_remote AS INT) AS is_remote_flag,
        CAST(is_remote AS INT) AS is_remote_flag_2
    FROM employees
    WHERE hire_date > DATE '2023-05-29'
      AND job_title IS NOT DISTINCT FROM 'Analyst'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY RANK() OVER (PARTITION BY dept_id, manager_id ORDER BY is_remote_flag_2 NULLS LAST) = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(is_remote_flag) OVER w_part AS last4_min_is_remote_flag,
        COUNT(is_remote_flag) OVER w_part AS last4_count_is_remote_flag,
        SUM(is_remote_flag) OVER w_part AS last4_sum_is_remote_flag
    FROM deduped
    WHERE manager_id IN (1, 37)
    WINDOW w_part AS (PARTITION BY level ORDER BY salary, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach employees attributes
    SELECT
        e.id,
        e.full_name,
        e.dept_id,
        e.manager_id,
        e.salary,
        e.level,
        e.is_remote_flag_2,
        e.last4_min_is_remote_flag,
        e.last4_count_is_remote_flag,
        p.job_title,
        p.level AS employee_level,
        p.salary AS employee_salary
    FROM with_windows e
    LEFT JOIN employees p
        ON e.manager_id <=> p.id
)
SELECT
    s.id,
    s.full_name,
    s.dept_id,
    s.manager_id,
    s.salary,
    s.level,
    s.last4_min_is_remote_flag,
    s.last4_count_is_remote_flag,
    s.job_title,
    (SELECT SUM(e2.salary) FROM employees e2 WHERE e2.manager_id = s.manager_id) AS sum_employee_salary
FROM enriched s
ORDER BY id DESC NULLS FIRST
