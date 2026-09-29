WITH scored AS (
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        LEAD(salary, 1) OVER w_recent AS next_salary,
        ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date, id) AS event_seq
    FROM employees
    WINDOW w_recent AS (PARTITION BY job_title, level ORDER BY hire_date, id)
),
joined AS (
    SELECT
        src.id,
        src.full_name,
        src.manager_id,
        src.salary,
        src.hire_date,
        src.next_salary,
        d.email,
        d.hire_date AS employee_hire_date
    FROM scored src
    LEFT JOIN employees d
        ON src.manager_id = d.id
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY manager_id ORDER BY employee_hire_date DESC NULLS FIRST, id) AS rn
    FROM joined
    QUALIFY rn = 1
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY manager_id ORDER BY next_salary DESC NULLS LAST) AS row_num
    FROM deduped
    QUALIFY row_num = 1
)
SELECT
    id,
    full_name,
    manager_id,
    salary,
    next_salary,
    email,
    rn,
    row_num
FROM latest
ORDER BY next_salary DESC NULLS LAST
