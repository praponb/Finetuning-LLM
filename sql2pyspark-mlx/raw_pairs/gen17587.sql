WITH latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.hire_date
    FROM departments s
    JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.location,
        s.budget,
        s.hire_date,
        top1.salary,
        top1.employee_hire_date
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.salary, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date <= s.hire_date
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM with_last
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY salary, id) = 1
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.job_title NOT IN ('Intern', 'Manager')
        )
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        dept_name,
        location,
        hire_date,
        salary
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY employee_hire_date, id) AS rank_in_group
        FROM eligible
    ) x
    WHERE rank_in_group = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.dept_id IN (17, 22, 29, 33)
        )
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        dept_name,
        COUNT(DISTINCT dept_name) AS distinct_dept_names,
        COUNT(CASE WHEN location = 'New York' THEN 1 END) AS new_york_count,
        SUM(CASE WHEN dept_name = 'Legal' THEN salary ELSE 0 END) AS legal_salary,
        APPROX_PERCENTILE(salary, 0.9) AS salary_p90,
        COUNT(DISTINCT location) AS distinct_locations,
        COUNT(*) FILTER (WHERE dept_name = 'Legal') AS legal_rows
    FROM not_flagged
    GROUP BY 1
    HAVING new_york_count < 1000
)
SELECT
    dept_name,
    new_york_count,
    salary_p90,
    distinct_locations,
    CASE WHEN dept_name RLIKE '^(HR|Marketing)$' THEN 'GROUP_A' ELSE 'CORE' END AS dept_name_zone
FROM totals
