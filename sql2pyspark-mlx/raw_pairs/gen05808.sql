WITH valid AS (
    SELECT *, CASE WHEN dept_name IN ('Marketing', 'Finance') THEN 'Priority' ELSE 'Other' END AS dept_name_group
    FROM departments
    WHERE budget <= 2733000
),
filtered AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        dept_name_group,
        LENGTH(dept_name) AS dept_name_length,
        TRIM(dept_name) AS clean_dept_name
    FROM valid
    WHERE budget > 1117000
),
with_windows AS (
    SELECT
        id,
        dept_name,
        budget,
        dept_name_group,
        clean_dept_name,
        NTILE(5) OVER (PARTITION BY dept_name ORDER BY budget DESC, id ASC) AS budget_bucket,
        LAST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name,
        ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget ASC, id ASC) AS seq_no,
        PERCENT_RANK() OVER (ORDER BY dept_name_length DESC) AS dept_name_length_percent_rank
    FROM filtered
    WHERE dept_name = 'Engineering'
),
latest_match AS (
    SELECT
        s.id,
        s.dept_name,
        s.budget,
        s.dept_name_group,
        s.clean_dept_name,
        s.dept_name_length_percent_rank,
        x.employee_salary
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    SELECT *
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY dept_name_length_percent_rank DESC NULLS FIRST, id ASC) = 1
),
eligible AS (
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.salary < 146000
        )
)
SELECT
    dept_name,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(dept_name_length_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS dept_name_length_percent_rank_quartiles,
    SUM(employee_salary) AS total_employee_salary,
    COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END) AS marketing_count
FROM eligible
GROUP BY CUBE (dept_name)
ORDER BY num_dept_names DESC;
