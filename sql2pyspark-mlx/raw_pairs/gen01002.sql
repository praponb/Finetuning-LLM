WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_name,
        SUM(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_budget,
        MAX(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_budget,
        MIN(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_budget,
        LAG(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget
    FROM departments
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(last7_min_budget, 1) AS last7_min_budget_rounded
    FROM enriched
    WHERE dept_name IS NOT DISTINCT FROM 'Engineering'
),
latest_match AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.last7_min_budget_rounded,
        top1.job_title
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY last7_min_budget_rounded DESC NULLS FIRST, id) = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY last7_min_budget_rounded, id) = 1
)
SELECT
    id,
    dept_name,
    last7_min_budget_rounded
FROM ranked
ORDER BY last7_min_budget_rounded NULLS FIRST;
