WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(is_remote AS INT) AS is_remote_flag,
        COALESCE(level, 'N/A') AS level_filled
    FROM employees
    WHERE dept_id = 27
      AND hire_date > TO_DATE('2023-07-22')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        manager_id,
        salary,
        hire_date,
        is_remote,
        level,
        level_filled,
        MIN(is_remote_flag) OVER w_grp AS running_min_is_remote_flag,
        COUNT(is_remote_flag) OVER w_grp AS running_count_is_remote_flag,
        AVG(is_remote_flag) OVER w_grp AS running_avg_is_remote_flag,
        LEAD(is_remote_flag, 2) OVER (PARTITION BY YEAR(hire_date), level ORDER BY salary ASC, id ASC) AS next_is_remote_flag
    FROM valid
    WINDOW w_grp AS (PARTITION BY dept_id ORDER BY is_remote_flag ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        salary,
        is_remote,
        level,
        running_count_is_remote_flag,
        running_avg_is_remote_flag,
        next_is_remote_flag,
        ROUND(running_avg_is_remote_flag, 1) AS running_avg_is_remote_flag_rounded
    FROM with_windows
    WHERE hire_date BETWEEN '2023-09-01' AND '2024-06-11'
      AND level IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        salary,
        is_remote,
        running_avg_is_remote_flag,
        next_is_remote_flag,
        CEIL(next_is_remote_flag) AS next_is_remote_flag_whole
    FROM cleaned
    WHERE salary BETWEEN 85000 AND 128000
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM filtered
    EXCEPT
    SELECT id
    FROM employees
    WHERE dept_id IN (16, 20, 22, 24)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    email,
    salary,
    is_remote,
    running_avg_is_remote_flag,
    next_is_remote_flag_whole
FROM verified
ORDER BY salary DESC
