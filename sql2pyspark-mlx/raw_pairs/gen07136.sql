WITH cleaned AS (
    SELECT *, ROUND(budget * 1.15, 2) AS budget_scaled
    FROM departments
    WHERE budget > 2702000
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget_scaled DESC) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
with_latest AS (
    SELECT
        s.id,
        s.dept_name,
        s.budget,
        s.budget_scaled,
        s.row_num,
        latest.level
    FROM deduped s
    JOIN LATERAL (
            SELECT c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest_departments AS (
    SELECT
        s.id,
        s.dept_name,
        s.budget,
        s.row_num,
        latest.employee_hire_date,
        latest.job_title
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.hire_date AS employee_hire_date, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    SELECT
        id,
        dept_name,
        budget,
        employee_hire_date,
        LAG(budget, 2) OVER (ORDER BY budget, id) AS prev_budget,
        SUM(budget) OVER w_recent AS cumulative_budget,
        LAG(budget, 1) OVER (PARTITION BY job_title ORDER BY employee_hire_date, id) AS prev_budget_2
    FROM with_latest_departments
    WHERE budget >= 3701000
    WINDOW w_recent AS (PARTITION BY job_title ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    dept_name,
    budget,
    employee_hire_date,
    prev_budget_2,
    LAG(budget) OVER w_grp AS prev_budget_3,
    LAG(prev_budget_2) OVER w_grp AS prev_prev_budget_2,
    ROW_NUMBER() OVER w_grp AS event_seq
FROM scored
WINDOW w_grp AS (PARTITION BY dept_name ORDER BY prev_budget, id);
