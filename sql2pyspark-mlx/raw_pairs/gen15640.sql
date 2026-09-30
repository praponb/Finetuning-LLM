WITH valid AS (
    -- Basic cleansing and defaults
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
        CASE WHEN job_title IN ('Designer', 'Director') THEN 'Tier 1' ELSE 'Other' END job_title_group
    FROM employees
    WHERE hire_date > DATE '2023-07-16'
      AND hire_date <= '2023-06-18'
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM valid
    EXCEPT ALL
    SELECT id
    FROM employees
    WHERE manager_id IS NOT NULL
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.id = k.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        full_name,
        email,
        hire_date,
        is_remote,
        level,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id, manager_id), 4) salary_share,
        PERCENT_RANK() OVER (ORDER BY salary DESC) salary_percent_rank,
        SUM(salary) OVER (PARTITION BY level) level_sum_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY dept_id), 4) salary_share_2,
        PERCENT_RANK() OVER (PARTITION BY dept_id, manager_id ORDER BY salary ASC) salary_percent_rank_2
    FROM kept_rows
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        email,
        hire_date,
        is_remote,
        level,
        salary_share,
        salary_percent_rank,
        level_sum_salary,
        salary_share_2,
        salary_percent_rank_2,
        ROUND(level_sum_salary * 1.15, 2) adjusted_level_sum_salary,
        COALESCE(level, 'NONE') level_filled
    FROM scored
    WHERE hire_date > DATE '2023-08-25'
),
cleaned_employees AS (
    -- Keep recent, valid rows
    SELECT
        is_remote,
        salary_percent_rank,
        level_sum_salary,
        salary_share_2,
        salary_percent_rank_2,
        level_filled,
        ROUND(adjusted_level_sum_salary, 0) adjusted_level_sum_salary_rounded,
        COALESCE(level, 'NONE') resolved_level
    FROM cleaned
    WHERE hire_date >= TO_DATE('2023-08-07')
      AND level IS NOT NULL
)
SELECT
    is_remote,
    salary_percent_rank,
    level_sum_salary,
    salary_share_2,
    salary_percent_rank_2,
    level_filled,
    resolved_level
FROM cleaned_employees
ORDER BY salary_share_2 ASC
