WITH valid AS (
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        UPPER(email) AS email_upper,
        DATE_FORMAT(hire_date, 'MMM yyyy') AS hire_date_label,
        COALESCE(level, 'NONE') AS level_clean
    FROM employees
    WHERE hire_date >= TO_DATE('2023-03-26')
      AND level IS DISTINCT FROM 'L2'
)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary, id) AS rank_in_group
    FROM valid
) t
WHERE rank_in_group = 1
