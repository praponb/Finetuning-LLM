WITH cleaned AS (
    SELECT *, LOWER(dept_name) AS dept_name_lower
    FROM departments
    WHERE location IS NOT NULL
),
metrics AS (
    SELECT
        id,
        dept_name,
        location,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_sum_budget,
        MIN(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_budget,
        MAX(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_budget,
        COUNT(budget) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_budget
    FROM cleaned
),
scored AS (
    SELECT
        *,
        MAX(last3_count_budget) OVER w_part AS last7_max_last3_count_budget,
        MIN(last3_count_budget) OVER w_part AS last7_min_last3_count_budget,
        COUNT(last3_count_budget) OVER w_part AS last7_count_last3_count_budget,
        LAG(last3_sum_budget, 2) OVER w_part_2 AS prev_last3_sum_budget
    FROM metrics
    WHERE last3_min_budget > 2296000
    WINDOW w_part AS (PARTITION BY location, dept_name ORDER BY last3_sum_budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_part_2 AS (PARTITION BY location ORDER BY last3_max_budget, id)
),
with_latest AS (
    SELECT
        s.*,
        x.job_title,
        x.employee_hire_date
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.job_title, c.hire_date AS employee_hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics_departments AS (
    SELECT
        *,
        LEAD(prev_last3_sum_budget, 2, 0) OVER (PARTITION BY YEAR(employee_hire_date), location ORDER BY last3_min_budget, id) AS next_prev_last3_sum_budget,
        LEAD(last7_count_last3_count_budget, 1) OVER (PARTITION BY dept_name ORDER BY prev_last3_sum_budget, id) AS next_last7_count_last3_count_budget
    FROM with_latest
),
by_type AS (
    SELECT *
    FROM (
        SELECT
            location,
            dept_name,
            last7_max_last3_count_budget
        FROM metrics_departments
    ) src
    PIVOT (
        MAX(last7_max_last3_count_budget)
        FOR dept_name IN ('Legal', 'HR')
    )
)
SELECT location, Legal
FROM by_type
ORDER BY Legal NULLS FIRST, location DESC;
