WITH joined AS (
    -- Attach employees attributes
    SELECT
        a.id,
        a.full_name,
        a.email,
        a.manager_id,
        a.job_title,
        a.salary,
        a.hire_date,
        a.is_remote,
        a.level,
        e.email AS employee_email,
        e.job_title AS employee_job_title,
        e.full_name AS employee_full_name
    FROM employees a
    JOIN employees e
        ON a.manager_id = e.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        full_name,
        job_title,
        hire_date,
        is_remote,
        employee_job_title,
        employee_full_name
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        job_title,
        hire_date,
        employee_job_title,
        LAST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        FIRST_VALUE(employee_job_title, TRUE) OVER (PARTITION BY employee_job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_employee_job_title,
        LAST_VALUE(employee_job_title, TRUE) OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_employee_job_title
    FROM latest
    WHERE NOT is_remote
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        last_known_job_title,
        first_known_employee_job_title,
        ROW_NUMBER() OVER (PARTITION BY job_title, employee_job_title ORDER BY hire_date DESC, id) AS row_seq,
        FIRST_VALUE(employee_job_title) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_employee_job_title_2,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date, id) AS seq_no
    FROM scored
),
report AS (
    SELECT
        id,
        full_name,
        last_known_job_title,
        first_known_employee_job_title,
        first_known_employee_job_title_2,
        CASE WHEN full_name RLIKE '^(Jack Taylor|David Lee )$' THEN 'GROUP_B' WHEN full_name RLIKE '^(Alice Wong|bob smith)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS full_name_zone
    FROM scored_employees
    ORDER BY id DESC, full_name_zone
)
SELECT
    id,
    full_name,
    last_known_job_title,
    first_known_employee_job_title,
    first_known_employee_job_title_2
FROM report
ORDER BY id NULLS FIRST;
