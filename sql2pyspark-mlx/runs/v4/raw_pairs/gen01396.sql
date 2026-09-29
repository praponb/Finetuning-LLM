WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(is_remote AS INT) is_remote_flag,
        LENGTH(full_name) full_name_length
    FROM employees
    WHERE hire_date > TO_DATE('2023-05-31')
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        a.*,
        d.budget,
        d.location,
        d.dept_name
    FROM valid a
    INNER JOIN departments d
        ON a.dept_id = d.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        salary - is_remote_flag salary_minus_is_remote_flag,
        CONCAT(dept_name, '/', job_title) dept_name_job_title
    FROM with_dims
    WHERE hire_date BETWEEN '2023-10-01' AND '2025-03-01'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY budget DESC NULLS FIRST) budget_cume_dist,
        AVG(salary) OVER (PARTITION BY dept_id, location) dept_id_avg_salary,
        LAG(is_remote_flag, 2) OVER (PARTITION BY job_title ORDER BY budget, id) prev_is_remote_flag
    FROM cleaned
),
kept_keys AS (
    -- Set intersection on keys
    SELECT manager_id, id
    FROM enriched
    INTERSECT ALL
    SELECT manager_id, id
    FROM employees
    WHERE email LIKE '%@startup.io'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.manager_id = k.manager_id AND s.id = k.id
),
joined AS (
    -- Attach departments attributes
    SELECT s.*, b.dept_name AS department_dept_name
    FROM verified s
    LEFT JOIN departments b
        ON s.dept_id = b.id
),
enriched_employees AS (
    -- Attach departments attributes
    SELECT
        a.*,
        d.dept_name AS department_dept_name_2,
        d.budget AS department_budget,
        d.location AS department_location
    FROM joined a
    LEFT JOIN departments d
        ON a.dept_id = d.id
)
-- Grouped metrics
SELECT
    CASE WHEN prev_is_remote_flag >= 61 THEN 'HIGH' ELSE 'LOW' END AS prev_is_remote_flag_tier,
    DATE_TRUNC('WEEK', hire_date) AS week_start,
    ROUND(AVG(salary), 2) AS avg_salary,
    SUM(CASE WHEN dept_name = 'Engineering' THEN salary_minus_is_remote_flag ELSE 0 END) AS engineering_salary_minus_is_remote_flag,
    SUM(CASE WHEN location = 'Berlin' THEN prev_is_remote_flag ELSE 0 END) AS berlin_prev_is_remote_flag,
    APPROX_PERCENTILE(salary_minus_is_remote_flag, ARRAY(0.25, 0.5, 0.75)) AS salary_minus_is_remote_flag_quartiles,
    GROUPING_ID() AS gid
FROM enriched_employees
GROUP BY CUBE (CASE WHEN prev_is_remote_flag >= 61 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('WEEK', hire_date))
ORDER BY engineering_salary_minus_is_remote_flag DESC
