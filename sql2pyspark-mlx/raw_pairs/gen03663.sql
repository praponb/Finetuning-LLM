WITH valid AS (
    SELECT
        *,
        LOWER(level) AS level_lower,
        INITCAP(TRIM(job_title)) AS job_title_title,
        COALESCE(level, 'DIRECT') AS level_filled
    FROM employees
    WHERE hire_date > DATE '2023-09-12'
      AND manager_id IS NOT NULL
)
SELECT *
FROM (
    SELECT
        manager_id,
        level,
        salary
    FROM valid
) src
PIVOT (
    MAX(salary)
    FOR level IN ('L1' AS l1, 'L5' AS l5, 'L3' AS l3)
)
