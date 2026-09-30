WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM departments s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.level IS NOT NULL
        )
),
with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.dept_name,
        s.budget,
        x.salary,
        x.employee_level
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.salary, c.level AS employee_level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(salary) OVER w_hist AS last7_max_salary,
        AVG(salary) OVER w_hist AS last7_avg_salary,
        SUM(salary) OVER w_hist AS last7_sum_salary
    FROM with_latest
    WHERE employee_level = 'L4'
    WINDOW w_hist AS (PARTITION BY employee_level, dept_name ORDER BY budget, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        salary,
        last7_avg_salary,
        SUM(last7_avg_salary) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_last7_avg_salary,
        MIN(last7_avg_salary) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_last7_avg_salary,
        COUNT(last7_avg_salary) OVER (PARTITION BY dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_last7_avg_salary,
        AVG(last7_max_salary) OVER (ORDER BY last7_max_salary, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS last7_max_salary_avg_5
    FROM with_windows
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.job_title IS NOT NULL
        )
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN running_min_last7_avg_salary >= 151000 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_min_last7_avg_salary_tier,
        CASE WHEN last7_avg_salary >= 122000 THEN 'HIGH' ELSE 'LOW' END AS last7_avg_salary_tier,
        CASE WHEN salary >= 154000 THEN 'PREMIUM' ELSE 'STANDARD' END AS salary_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(AVG(salary), 2) AS avg_salary,
        ROUND(AVG(last7_max_salary_avg_5), 2) AS avg_last7_max_salary_avg_5
    FROM not_flagged
    GROUP BY CASE WHEN running_min_last7_avg_salary >= 151000 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN last7_avg_salary >= 122000 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN salary >= 154000 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    running_min_last7_avg_salary_tier,
    last7_avg_salary_tier,
    salary_tier,
    distinct_ids,
    avg_salary
FROM totals;
