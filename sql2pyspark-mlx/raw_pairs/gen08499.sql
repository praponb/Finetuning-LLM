WITH with_dims AS (
    -- Attach departments attributes
    SELECT
        a.*,
        p.location,
        p.dept_name
    FROM employees a
    LEFT JOIN departments p
        ON a.dept_id = p.id
    WHERE p.budget <= 1372000
),
with_dims_employees AS (
    -- Attach employees attributes
    SELECT
        src.*,
        e.full_name AS employee_full_name,
        e.salary AS employee_salary,
        e.email AS employee_email
    FROM with_dims src
    LEFT JOIN employees e
        ON src.manager_id = e.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        dept_id,
        manager_id,
        job_title,
        level,
        location,
        dept_name,
        employee_full_name,
        SUM(salary) OVER w_recent AS cumulative_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id, location), 4) AS salary_share,
        SUM(salary) OVER (ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_salary_2,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS row_seq,
        NTILE(10) OVER w_ordered AS employee_salary_bucket
    FROM with_dims_employees
    WINDOW w_recent AS (PARTITION BY level ORDER BY employee_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY location ORDER BY employee_salary, id)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        dept_id,
        job_title,
        level,
        location,
        dept_name,
        employee_full_name,
        cumulative_salary,
        salary_share,
        row_seq
    FROM (
        SELECT *, RANK() OVER (PARTITION BY dept_name, manager_id ORDER BY cumulative_salary DESC NULLS LAST) AS dedup_rank
        FROM scored
    ) t
    WHERE dedup_rank = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT dept_id, id
    FROM deduped
    EXCEPT
    SELECT dept_id, id
    FROM employees
    WHERE hire_date >= '2024-03-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.dept_id = k.dept_id AND s.id = k.id
),
enriched AS (
    -- Attach departments attributes
    SELECT s.*, d.dept_name AS department_dept_name
    FROM reconciled s
    INNER JOIN departments d
        ON s.dept_id = d.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(salary_share) OVER (PARTITION BY job_title) AS job_title_avg_salary_share,
        PERCENT_RANK() OVER (ORDER BY cumulative_salary DESC) AS cumulative_salary_percent_rank,
        MIN(salary_share) OVER (PARTITION BY department_dept_name) AS department_dept_name_min_salary_share,
        CUME_DIST() OVER (PARTITION BY dept_id ORDER BY salary_share DESC NULLS FIRST) AS salary_share_cume_dist
    FROM enriched
)
SELECT
    s.id,
    s.dept_id,
    s.job_title,
    s.level,
    s.location,
    s.dept_name,
    s.employee_full_name,
    s.row_seq,
    s.job_title_avg_salary_share,
    (SELECT MIN(x.salary) FROM employees x WHERE x.dept_id = s.dept_id) AS min_employee_salary
FROM with_windows s
ORDER BY row_seq DESC NULLS LAST
