WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        LENGTH(email) AS email_length
    FROM employees
    WHERE level IN ('L3', 'L2')
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        dept_id,
        manager_id,
        job_title,
        email_length,
        REGEXP_REPLACE(full_name, ' ', '_') AS full_name_slug,
        CASE WHEN job_title = 'Intern' THEN 1 ELSE 0 END AS is_intern
    FROM cleaned
    WHERE manager_id IS NOT NULL
      AND email_length = 61
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(email_length) OVER (PARTITION BY manager_id, dept_id ORDER BY email_length ASC, id ASC) AS next_email_length,
        LAG(email_length) OVER (PARTITION BY manager_id, dept_id ORDER BY email_length ASC, id ASC) AS prev_email_length,
        LEAD(is_intern) OVER (PARTITION BY manager_id, dept_id ORDER BY email_length ASC, id ASC) AS next_is_intern,
        LAG(is_intern) OVER (PARTITION BY manager_id, dept_id ORDER BY email_length ASC, id ASC) AS prev_is_intern,
        ROW_NUMBER() OVER (PARTITION BY manager_id, dept_id ORDER BY email_length ASC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY job_title, dept_id ORDER BY email_length ASC, id ASC) AS seq_no
    FROM base
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(prev_is_intern) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS prev_is_intern_avg_6_prior,
        SUM(prev_is_intern) OVER (PARTITION BY job_title ORDER BY seq_no ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_is_intern,
        LAST_VALUE(next_is_intern, TRUE) OVER (PARTITION BY manager_id ORDER BY prev_email_length ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_is_intern
    FROM enriched
)
SELECT
    s.id,
    s.full_name,
    s.manager_id,
    s.job_title,
    s.email_length,
    s.full_name_slug,
    s.is_intern,
    s.next_email_length,
    s.prev_email_length,
    CASE WHEN full_name RLIKE '^(David Lee |Mia Rossi)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS full_name_zone,
    (SELECT MAX(e2.salary) FROM employees e2 WHERE e2.manager_id = s.manager_id) AS max_employee_salary
FROM scored s
ORDER BY prev_email_length DESC NULLS LAST;
