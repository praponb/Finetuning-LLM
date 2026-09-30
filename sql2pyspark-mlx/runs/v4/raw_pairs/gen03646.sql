WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget,
        NTILE(10) OVER w_ordered AS budget_bucket,
        AVG(budget) OVER w_recent AS budget_avg_2_prior,
        AVG(budget) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS budget_avg_2,
        LAST_VALUE(budget) IGNORE NULLS OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget
    FROM departments
    WINDOW w_ordered AS (ORDER BY budget, id), w_recent AS (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.salary,
        latest.job_title
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.salary, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(location, TRUE) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_location,
        LEAD(budget_avg_2, 2) OVER (PARTITION BY dept_name ORDER BY last_known_budget, id) AS next_budget_avg_2,
        ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY budget_avg_2 DESC, id) AS row_seq,
        CUME_DIST() OVER (PARTITION BY job_title, dept_name ORDER BY prev_budget NULLS LAST) AS prev_budget_cume_dist,
        LAST_VALUE(dept_name, TRUE) OVER (PARTITION BY dept_name ORDER BY budget_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name
    FROM with_last
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY next_budget_avg_2 DESC, id) AS row_num
        FROM with_windows
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    location,
    budget,
    prev_budget,
    budget_bucket,
    budget_avg_2_prior,
    budget_avg_2,
    job_title,
    first_known_location
FROM latest
ORDER BY budget_avg_2_prior NULLS FIRST, prev_budget
