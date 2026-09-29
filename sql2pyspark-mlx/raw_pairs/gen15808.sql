WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_id,
        hire_date,
        level,
        SUM(salary) OVER (PARTITION BY job_title ORDER BY salary ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary,
        AVG(salary) OVER (PARTITION BY job_title ORDER BY hire_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS salary_avg_3,
        ROUND(salary / SUM(salary) OVER (PARTITION BY level, manager_id), 4) AS salary_share,
        AVG(salary) OVER (PARTITION BY job_title, manager_id ORDER BY hire_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS salary_avg_5,
        ROW_NUMBER() OVER (PARTITION BY dept_id, job_title ORDER BY salary DESC, id ASC) AS event_seq
    FROM employees
),
joined AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.dept_id,
        a.level,
        a.salary_avg_5,
        a.event_seq,
        d.location
    FROM scored a
    INNER JOIN departments d
        ON a.dept_id = d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_id,
        location,
        AVG(salary_avg_5) OVER (ORDER BY salary_avg_5 ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS salary_avg_5_avg_6_prior,
        SUM(event_seq) OVER (PARTITION BY dept_id, location ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_seq,
        AVG(salary_avg_5) OVER (PARTITION BY location ORDER BY salary_avg_5 ASC, id ASC ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS salary_avg_5_avg_2_prior,
        SUM(salary_avg_5) OVER (PARTITION BY level ORDER BY event_seq ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_avg_5
    FROM joined
    WHERE salary_avg_5 != 137646
),
with_dims AS (
    -- Attach departments attributes
    SELECT s.*, d.dept_name
    FROM metrics s
    INNER JOIN departments d
        ON s.dept_id = d.id
),
with_dims_employees AS (
    -- Attach departments attributes
    SELECT
        e.*,
        b.location AS department_location,
        b.budget,
        b.dept_name AS department_dept_name
    FROM with_dims e
    INNER JOIN departments b
        ON e.dept_id = b.id
),
joined_employees AS (
    -- Attach departments attributes
    SELECT s.*, d.location AS department_location_2
    FROM with_dims_employees s
    INNER JOIN departments d
        ON s.dept_id = d.id
)
-- Grouped metrics
SELECT
    department_dept_name,
    CASE WHEN salary_avg_5_avg_6_prior >= 157080 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_avg_5_avg_6_prior_tier,
    CASE WHEN running_event_seq >= 38 THEN 'LARGE' ELSE 'SMALL' END AS running_event_seq_tier,
    SORT_ARRAY(COLLECT_SET(department_location)) AS department_location_list,
    PERCENTILE_APPROX(cumulative_salary_avg_5, 0.5) AS cumulative_salary_avg_5_p50,
    SUM(CASE WHEN location = 'Bangkok' THEN salary_avg_5_avg_2_prior ELSE 0 END) AS bangkok_salary_avg_5_avg_2_prior,
    APPROX_PERCENTILE(cumulative_salary_avg_5, ARRAY(0.25, 0.5, 0.75)) AS cumulative_salary_avg_5_quartiles,
    GROUPING(department_dept_name) AS gid
FROM joined_employees
GROUP BY GROUPING SETS ((department_dept_name, CASE WHEN salary_avg_5_avg_6_prior >= 157080 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN running_event_seq >= 38 THEN 'LARGE' ELSE 'SMALL' END), (department_dept_name, CASE WHEN salary_avg_5_avg_6_prior >= 157080 THEN 'PREMIUM' ELSE 'STANDARD' END))
HAVING PERCENTILE_APPROX(cumulative_salary_avg_5, 0.5) > 0;
