WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        full_name,
        email,
        dept_id,
        job_title,
        salary,
        hire_date
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title ORDER BY salary ASC, id ASC) AS rn
        FROM employees
    ) ranked
    WHERE rn = 1
),
joined AS (
    -- Attach departments attributes
    SELECT
        src.*,
        b.location,
        b.dept_name
    FROM deduped src
    JOIN departments b
        ON src.dept_id = b.id
),
with_dims AS (
    -- Attach departments attributes
    SELECT
        src.*,
        p.location AS department_location,
        p.budget,
        p.dept_name AS department_dept_name
    FROM joined src
    JOIN departments p
        ON src.dept_id = p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(budget, TRUE) OVER (ORDER BY hire_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_budget,
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY budget ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS salary_avg_5,
        NTILE(5) OVER w_recent AS budget_bucket,
        SUM(budget) OVER (ORDER BY budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget,
        DENSE_RANK() OVER (ORDER BY budget ASC NULLS LAST) AS budget_dense_rank
    FROM with_dims
    WHERE LENGTH(email) > 7
    WINDOW w_recent AS (PARTITION BY department_location ORDER BY budget DESC, id ASC)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        job_title,
        location,
        dept_name,
        department_location,
        department_dept_name,
        first_known_budget,
        FIRST_VALUE(dept_name, TRUE) OVER (ORDER BY first_known_budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_dept_name,
        ROUND(budget_bucket / SUM(budget_bucket) OVER (PARTITION BY department_location), 4) AS budget_bucket_share
    FROM metrics
),
enriched_employees AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(first_known_budget) OVER w_recent AS prev_first_known_budget,
        LEAD(budget_bucket_share) OVER w_recent AS next_budget_bucket_share,
        LAG(budget_bucket_share) OVER w_recent AS prev_budget_bucket_share,
        ROW_NUMBER() OVER w_recent AS event_seq,
        FIRST_VALUE(first_known_budget) IGNORE NULLS OVER (ORDER BY first_known_budget ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_budget
    FROM enriched
    WINDOW w_recent AS (PARTITION BY job_title ORDER BY budget_bucket_share ASC, id ASC)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        job_title,
        department_dept_name,
        budget_bucket_share,
        prev_first_known_budget,
        next_budget_bucket_share,
        prev_budget_bucket_share,
        event_seq,
        DENSE_RANK() OVER (ORDER BY budget_bucket_share DESC NULLS LAST) AS budget_bucket_share_dense_rank,
        ROUND(first_known_first_known_budget / SUM(first_known_first_known_budget) OVER (PARTITION BY location), 4) AS first_known_first_known_budget_share
    FROM enriched_employees
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN event_seq >= 9 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
        CASE WHEN prev_budget_bucket_share >= 0.66 THEN 'HIGH' ELSE 'LOW' END AS prev_budget_bucket_share_tier,
        SUM(CASE WHEN department_dept_name = 'Sales' THEN budget_bucket_share_dense_rank ELSE 0 END) AS sales_budget_bucket_share_dense_rank,
        COUNT(DISTINCT job_title) AS distinct_job_titles,
        COUNT(*) FILTER (WHERE job_title = 'Director') AS director_rows,
        COUNT(*) AS cnt
    FROM scored
    GROUP BY GROUPING SETS ((CASE WHEN event_seq >= 9 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN prev_budget_bucket_share >= 0.66 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN event_seq >= 9 THEN 'HIGH' ELSE 'LOW' END), ())
)
SELECT
    prev_budget_bucket_share_tier,
    sales_budget_bucket_share_dense_rank,
    director_rows
FROM summary
ORDER BY prev_budget_bucket_share_tier DESC NULLS LAST;
