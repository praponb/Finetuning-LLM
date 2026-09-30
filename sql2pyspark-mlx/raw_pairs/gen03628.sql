WITH cleaned AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        level,
        DATEDIFF(CURRENT_DATE(), hire_date) / 7 AS weeks_since_hire_date,
        COALESCE(level, 'N/A') AS resolved_level
    FROM employees
    WHERE hire_date > TO_DATE('2023-06-30')
      AND level IS NOT NULL
)
SELECT *
FROM (
    SELECT
        id,
        level,
        salary,
        weeks_since_hire_date
    FROM cleaned
) src
UNPIVOT (
    reading FOR metric_name IN (salary, weeks_since_hire_date)
) long_rows
