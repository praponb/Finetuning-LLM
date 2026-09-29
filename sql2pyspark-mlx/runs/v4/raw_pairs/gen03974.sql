WITH base AS (
    -- Keep recent, valid rows
    SELECT *, LAST_DAY(hire_date) AS hire_date_month_end
    FROM employees
    WHERE hire_date >= DATE '2023-05-01'
      AND manager_id IS NOT NULL
),
enriched AS (
    -- Attach departments attributes
    SELECT
        e.*,
        d.location,
        d.dept_name
    FROM base e
    INNER JOIN departments d
        ON e.dept_id = d.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        dept_id,
        salary,
        hire_date,
        hire_date_month_end,
        location,
        dept_name,
        CONCAT_WS(',', email, location) AS email_location_key
    FROM enriched
    WHERE dept_id = 33
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', hire_date) AS year_start,
    DATE_TRUNC('MONTH', hire_date_month_end) AS activity_month,
    ROUND(AVG(salary), 2) AS avg_salary,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN dept_name = 'Finance' THEN 1 END), 0), 2) AS salary_per_finance,
    SUM(salary) FILTER (WHERE location = 'Chiang Mai') AS chiang_mai_total_salary
FROM cleaned
GROUP BY CUBE (DATE_TRUNC('YEAR', hire_date), DATE_TRUNC('MONTH', hire_date_month_end))
ORDER BY activity_month DESC NULLS LAST
