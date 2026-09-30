WITH joined AS (
    -- Attach departments attributes
    SELECT
        e.id,
        e.full_name,
        e.email,
        e.dept_id,
        e.manager_id,
        e.job_title,
        e.salary,
        d.budget
    FROM employees e
    LEFT JOIN departments d
        ON e.dept_id = d.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        full_name,
        job_title,
        salary,
        budget
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id, manager_id ORDER BY salary, id) AS row_num
        FROM joined
    ) x
    WHERE row_num = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY budget NULLS LAST, id) AS rank_in_group
        FROM latest
    ) t
    WHERE rank_in_group = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        full_name,
        budget,
        rank_in_group,
        COUNT(salary) OVER (PARTITION BY job_title) AS job_title_count_salary,
        MIN(budget) OVER (PARTITION BY job_title) AS job_title_min_budget
    FROM ranked
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(job_title_count_salary) OVER (ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_job_title_count_salary,
        PERCENT_RANK() OVER (ORDER BY budget DESC) AS budget_percent_rank
    FROM metrics
),
scored_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        job_title_count_salary,
        AVG(job_title_min_budget) OVER (ORDER BY rank_in_group, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS job_title_min_budget_avg_6,
        RANK() OVER (ORDER BY job_title_min_budget) AS job_title_min_budget_rank,
        FIRST_VALUE(budget_percent_rank, TRUE) OVER (ORDER BY budget_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget_percent_rank,
        LAG(budget, 2, 0) OVER (ORDER BY budget_percent_rank, id) AS prev_budget
    FROM scored
    WHERE running_job_title_count_salary != 48
),
metrics_employees AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        job_title_count_salary,
        job_title_min_budget_avg_6,
        job_title_min_budget_rank,
        first_known_budget_percent_rank,
        ROW_NUMBER() OVER (ORDER BY job_title_count_salary DESC, id) AS row_seq,
        SUM(job_title_min_budget_rank) OVER (ORDER BY job_title_count_salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_job_title_min_budget_rank
    FROM scored_employees
)
SELECT
    id,
    full_name,
    job_title_count_salary,
    row_seq
FROM metrics_employees
ORDER BY job_title_count_salary DESC NULLS FIRST;
