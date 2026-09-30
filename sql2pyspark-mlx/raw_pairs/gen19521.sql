WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        full_name,
        manager_id,
        salary,
        hire_date,
        level
    FROM employees
    QUALIFY ROW_NUMBER() OVER (PARTITION BY level, manager_id ORDER BY salary DESC, id) = 1
),
with_dims AS (
    -- Attach employees attributes
    SELECT
        e.*,
        d.hire_date AS employee_hire_date,
        d.full_name AS employee_full_name,
        d.email
    FROM deduped e
    JOIN employees d
        ON e.manager_id <=> d.id
),
deduped_employees AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        manager_id,
        hire_date,
        email
    FROM (
        SELECT *, RANK() OVER (PARTITION BY level, manager_id ORDER BY hire_date NULLS FIRST) AS rank_in_group
        FROM with_dims
    ) t
    WHERE rank_in_group = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(email) AS email_upper
    FROM deduped_employees
    WHERE hire_date >= TO_DATE('2023-03-30')
      AND hire_date <= '2025-03-01'
),
joined AS (
    -- Attach employees attributes
    SELECT
        src.id,
        src.manager_id,
        src.hire_date,
        src.email,
        d.job_title
    FROM valid src
    JOIN employees d
        ON src.manager_id = d.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        ROW_NUMBER() OVER (PARTITION BY manager_id, job_title ORDER BY hire_date DESC, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY hire_date DESC, id) AS event_seq
    FROM joined
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics
    EXCEPT
    SELECT id
    FROM employees
    WHERE dept_id = 23
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    email,
    seq_no,
    event_seq
FROM verified
ORDER BY seq_no DESC NULLS LAST;
