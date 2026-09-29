WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        dept_id,
        salary,
        hire_date,
        is_remote,
        level,
        LAST_VALUE(job_title) IGNORE NULLS OVER (PARTITION BY dept_id ORDER BY hire_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_job_title,
        LEAD(salary, 1) OVER (PARTITION BY level ORDER BY salary, id) AS next_salary
    FROM employees
),
enriched AS (
    -- Attach departments attributes
    SELECT
        a.id,
        a.dept_id,
        a.hire_date,
        a.is_remote,
        d.dept_name
    FROM with_windows a
    LEFT JOIN departments d
        ON a.dept_id = d.id
    WHERE d.dept_name IN ('Engineering', 'Finance')
),
enriched_employees AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_id,
        hire_date,
        dept_name,
        ROW_NUMBER() OVER w_part AS seq_no,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM enriched
    WINDOW w_part AS (PARTITION BY dept_name ORDER BY hire_date DESC, id), w_grp AS (PARTITION BY dept_name ORDER BY hire_date, id)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        hire_date,
        seq_no,
        CASE WHEN dept_name = 'Sales' THEN 1 ELSE 0 END AS is_sales
    FROM enriched_employees
    WHERE hire_date >= DATE '2023-08-17'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN is_sales >= 60 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_sales_tier,
        MAX(is_sales) AS max_is_sales,
        ROUND(AVG(seq_no), 2) AS avg_seq_no,
        PERCENTILE_APPROX(is_sales, ARRAY(0.25, 0.5, 0.75)) AS is_sales_quartiles,
        APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles
    FROM base
    GROUP BY is_sales_tier
)
SELECT
    is_sales_tier,
    avg_seq_no,
    is_sales_quartiles
FROM stats
ORDER BY is_sales_tier DESC NULLS LAST, avg_seq_no
LIMIT 10;
