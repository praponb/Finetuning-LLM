WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        CONCAT_WS('/', email, level) AS email_level_key
    FROM employees
    WHERE hire_date > TO_DATE('2023-06-30')
      AND job_title NOT IN ('Manager', 'Engineer')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY salary DESC, id) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(salary / SUM(salary) OVER (PARTITION BY manager_id), 4) AS salary_share,
        ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS event_seq,
        LAST_VALUE(level, TRUE) OVER (PARTITION BY level ORDER BY latest_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_level
    FROM latest
    WHERE salary < 128000
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY event_seq DESC, id) AS rn
    FROM scored
    QUALIFY rn = 1
)
SELECT
    id,
    full_name,
    email,
    salary,
    level,
    latest_rank,
    salary_share,
    event_seq,
    last_known_level
FROM ranked
ORDER BY level, latest_rank
