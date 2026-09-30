WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS row_num
        FROM departments
    ) ranked
    WHERE row_num = 1
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.budget,
        latest.hire_date
    FROM latest s
    JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.hire_date,
        latest.level,
        latest.employee_salary
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.level, c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date <= s.hire_date
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        level,
        SUM(employee_salary) OVER (PARTITION BY level ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_employee_salary,
        ROUND(employee_salary / SUM(employee_salary) OVER (PARTITION BY level), 4) AS employee_salary_share,
        FIRST_VALUE(level, TRUE) OVER (PARTITION BY level ORDER BY employee_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        ROUND(employee_salary / SUM(employee_salary) OVER (PARTITION BY level), 4) AS employee_salary_share_2,
        LAG(employee_salary, 1, 0) OVER (PARTITION BY level ORDER BY employee_salary, id) AS prev_employee_salary
    FROM with_last
    WHERE level IN ('L3', 'L2', 'L1')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        level,
        running_employee_salary,
        employee_salary_share,
        first_known_level,
        prev_employee_salary,
        LAST_VALUE(employee_salary_share) IGNORE NULLS OVER w_grp AS last_known_employee_salary_share,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY prev_employee_salary, id) AS row_seq
    FROM enriched
    WINDOW w_grp AS (PARTITION BY level ORDER BY running_employee_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN running_employee_salary >= 681399 THEN 'LARGE' ELSE 'SMALL' END AS running_employee_salary_tier,
    level,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(employee_salary_share, 0.5) AS employee_salary_share_p50,
    APPROX_PERCENTILE(running_employee_salary, ARRAY(0.25, 0.5, 0.75)) AS running_employee_salary_quartiles,
    COUNT(CASE WHEN level = 'L1' THEN 1 END) AS l1_count,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN running_employee_salary >= 681399 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(*) DESC) AS event_count_percent_rank
FROM with_windows
GROUP BY CASE WHEN running_employee_salary >= 681399 THEN 'LARGE' ELSE 'SMALL' END, level
ORDER BY l1_count
