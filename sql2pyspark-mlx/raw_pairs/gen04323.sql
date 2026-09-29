WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN job_title = 'Director' THEN 1 ELSE 0 END AS is_director
    FROM employees
    WHERE hire_date >= DATE '2023-08-26'
      AND manager_id IS NOT NULL
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        s.id,
        s.dept_id,
        s.manager_id,
        s.salary,
        s.hire_date,
        d.budget,
        d.dept_name
    FROM filtered s
    LEFT JOIN departments d
        ON s.dept_id = d.id
),
filtered_employees AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_id,
        manager_id,
        hire_date,
        budget,
        dept_name,
        ROUND(budget, 1) AS budget_rounded,
        DATEDIFF(CURRENT_DATE(), hire_date) / 30 AS months_since_hire_date,
        COALESCE(dept_name, 'N/A') AS dept_name_filled
    FROM with_dims
    WHERE dept_name IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        budget_rounded / NULLIF(months_since_hire_date, 0) AS budget_rounded_per_months_since_hire_date,
        months_since_hire_date - budget_rounded AS months_since_hire_date_minus_budget_rounded
    FROM filtered_employees
    WHERE budget IS NOT NULL
      AND dept_name IS NOT DISTINCT FROM 'Legal'
),
enriched AS (
    -- Attach employees attributes
    SELECT
        e.id,
        e.manager_id,
        e.hire_date,
        e.budget,
        e.dept_name,
        e.months_since_hire_date,
        e.dept_name_filled,
        p.email,
        p.job_title,
        p.full_name
    FROM base e
    LEFT JOIN employees p
        ON e.manager_id <=> p.id
),
joined AS (
    -- Attach employees attributes
    SELECT src.*, p.email AS employee_email
    FROM enriched src
    LEFT JOIN employees p
        ON src.manager_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(dept_name) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_dept_name,
        CUME_DIST() OVER w_hist AS months_since_hire_date_cume_dist
    FROM joined
    WINDOW w_hist AS (PARTITION BY manager_id ORDER BY months_since_hire_date DESC NULLS LAST)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', hire_date) AS week_start,
    CASE WHEN budget >= 3518000 THEN 'PREMIUM' ELSE 'STANDARD' END AS budget_tier,
    COUNT(CASE WHEN job_title = 'Intern' THEN 1 END) AS intern_count,
    MIN(months_since_hire_date) AS min_months_since_hire_date,
    MAX(months_since_hire_date) AS max_months_since_hire_date,
    COUNT_IF(job_title = 'Designer') AS designer_events,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(months_since_hire_date, ARRAY(0.25, 0.5, 0.75)) AS months_since_hire_date_quartiles,
    RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', hire_date) ORDER BY COUNT_IF(job_title = 'Designer') DESC) AS designer_events_rank
FROM scored
GROUP BY 1, 2
