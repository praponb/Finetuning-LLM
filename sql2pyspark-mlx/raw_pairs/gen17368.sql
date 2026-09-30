WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY location, dept_name ORDER BY budget ASC, id ASC) AS budget_bucket,
        PERCENT_RANK() OVER (ORDER BY budget ASC NULLS FIRST) AS budget_percent_rank,
        PERCENT_RANK() OVER (ORDER BY budget ASC NULLS LAST) AS budget_percent_rank_2,
        SUM(budget) OVER (PARTITION BY dept_name ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget
    FROM departments
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        dept_name,
        budget,
        budget_bucket,
        budget_percent_rank,
        running_budget
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_percent_rank_2 DESC, id ASC) = 1
),
with_last AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.budget,
        s.budget_bucket,
        s.budget_percent_rank,
        s.running_budget,
        x.employee_salary,
        x.job_title
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.salary AS employee_salary, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last_departments AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        x.salary
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(dept_name) AS clean_dept_name,
        CAST(salary AS INT) AS salary_int
    FROM with_last_departments
    WHERE dept_name IS NOT DISTINCT FROM 'Marketing'
      AND dept_name IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        dept_name,
        salary,
        clean_dept_name
    FROM (
        SELECT *, RANK() OVER (PARTITION BY dept_name ORDER BY salary DESC NULLS FIRST) AS dedup_rank
        FROM valid
    ) ranked
    WHERE dedup_rank = 1
),
daily AS (
    -- Grouped metrics
    SELECT
        CASE WHEN salary >= 130000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
        COUNT(DISTINCT dept_name) AS distinct_dept_names,
        PERCENTILE_APPROX(salary, ARRAY(0.25, 0.5, 0.75)) AS salary_quartiles,
        SUM(CASE WHEN dept_name = 'HR' THEN salary ELSE 0 END) AS hr_salary,
        ROUND(AVG(salary), 2) AS avg_salary,
        AVG(salary) AS avg_salary_2,
        COUNT(DISTINCT dept_name) AS distinct_dept_names_2
    FROM latest
    GROUP BY CASE WHEN salary >= 130000 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    salary_tier,
    distinct_dept_names,
    salary_quartiles,
    hr_salary,
    avg_salary_2
FROM daily
ORDER BY distinct_dept_names ASC, hr_salary DESC
