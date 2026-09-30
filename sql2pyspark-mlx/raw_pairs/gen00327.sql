WITH with_dims AS (
    -- Attach employees attributes
    SELECT
        s.*,
        d.salary AS employee_salary,
        d.email AS employee_email
    FROM employees s
    LEFT JOIN employees d
        ON s.manager_id = d.id
    WHERE d.email LIKE '%@startup.io'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(employee_salary, 1) OVER (PARTITION BY YEAR(hire_date) ORDER BY employee_salary, id) AS next_employee_salary,
        AVG(salary) OVER w_running AS salary_avg_2
    FROM with_dims
    WINDOW w_running AS (PARTITION BY job_title ORDER BY employee_salary, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(employee_salary) OVER (PARTITION BY manager_id ORDER BY employee_salary, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_employee_salary,
        MIN(employee_salary) OVER (PARTITION BY manager_id ORDER BY employee_salary, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_employee_salary,
        MAX(employee_salary) OVER (PARTITION BY manager_id ORDER BY employee_salary, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_employee_salary
    FROM with_windows
),
with_dims_employees AS (
    -- Attach departments attributes
    SELECT
        s.*,
        d.budget,
        d.location
    FROM scored s
    LEFT JOIN departments d
        ON s.dept_id = d.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY last4_sum_employee_salary NULLS LAST, id) AS row_num
    FROM with_dims_employees
    QUALIFY row_num = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(row_num, 1) AS row_num_rounded,
        COALESCE(location, 'OTHER') AS location_clean
    FROM ranked
    WHERE hire_date > DATE '2023-04-03'
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        dept_id,
        salary,
        employee_salary,
        employee_email,
        next_employee_salary,
        last4_sum_employee_salary,
        last4_min_employee_salary,
        last4_max_employee_salary,
        location,
        row_num_rounded,
        location_clean,
        NTILE(5) OVER (PARTITION BY location ORDER BY last4_min_employee_salary, id) AS last4_min_employee_salary_bucket,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY MONTH(hire_date), manager_id ORDER BY last4_max_employee_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level
    FROM valid
)
SELECT
    s.id,
    s.dept_id,
    s.salary,
    s.employee_salary,
    s.employee_email,
    s.next_employee_salary,
    s.last4_sum_employee_salary,
    s.last4_min_employee_salary,
    s.last4_max_employee_salary,
    (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.dept_id = s.dept_id) AS avg_employee_salary
FROM scored_employees s
ORDER BY avg_employee_salary DESC NULLS FIRST
