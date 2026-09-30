WITH valid AS (
    SELECT
        *,
        SUBSTRING(dept_name, 1, 4) AS dept_name_prefix,
        COALESCE(location, 'DIRECT') AS resolved_location
    FROM departments
    WHERE dept_name IS NOT NULL
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS dedup_rank
        FROM valid
    ) x
    WHERE dedup_rank = 1
),
enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) AS row_seq,
        NTILE(5) OVER w_running AS budget_bucket,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM deduped
    WHERE dedup_rank < 1
    WINDOW w_running AS (ORDER BY budget DESC, id), w_part AS (PARTITION BY location ORDER BY dedup_rank DESC, id)
),
latest AS (
    SELECT
        id,
        dept_name,
        resolved_location
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY dept_name, location ORDER BY row_seq DESC) = 1
),
latest_match AS (
    SELECT
        s.id,
        s.dept_name,
        x.salary
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
summary AS (
    SELECT
        CASE WHEN salary >= 121000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
        SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
        SUM(CASE WHEN dept_name = 'Finance' THEN salary ELSE 0 END) AS finance_salary,
        PERCENTILE_APPROX(salary, 0.95) AS salary_p95,
        COUNT(*) AS num_rows,
        SIZE(COLLECT_SET(dept_name)) AS num_dept_names,
        COUNT_IF(dept_name = 'Engineering') AS engineering_events
    FROM latest_match
    GROUP BY CASE WHEN salary >= 121000 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    salary_tier,
    finance_salary,
    num_rows,
    num_dept_names,
    engineering_events
FROM summary
ORDER BY salary_tier DESC NULLS FIRST;
