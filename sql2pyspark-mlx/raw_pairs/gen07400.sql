WITH valid AS (
    SELECT *, CASE WHEN dept_name = 'Sales' THEN 1 ELSE 0 END AS is_sales
    FROM departments
    WHERE dept_name IS NOT NULL
),
with_last AS (
    SELECT
        s.id,
        s.dept_name,
        x.salary,
        x.hire_date
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.salary, c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        id,
        salary,
        hire_date,
        SUM(salary) OVER w_recent AS last3_sum_salary,
        MAX(salary) OVER w_recent AS last3_max_salary,
        MIN(salary) OVER w_recent AS last3_min_salary
    FROM with_last
    WINDOW w_recent AS (PARTITION BY dept_name ORDER BY hire_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
filtered AS (
    SELECT *, CASE WHEN last3_sum_salary >= 749000 THEN 'High' WHEN last3_sum_salary >= 149000 THEN 'Medium' ELSE 'Low' END AS last3_sum_salary_band
    FROM metrics
    WHERE hire_date >= '2025-08-06'
),
metrics_departments AS (
    SELECT
        *,
        LEAD(last3_min_salary) OVER w_hist AS next_last3_min_salary,
        LEAD(last3_max_salary) OVER w_hist AS next_last3_max_salary,
        ROW_NUMBER() OVER w_hist AS row_seq,
        ROW_NUMBER() OVER (ORDER BY last3_sum_salary, id) AS event_seq
    FROM filtered
    WHERE MONTH(hire_date) IN (6, 7, 9)
    WINDOW w_hist AS (ORDER BY last3_min_salary, id)
),
kept AS (
    SELECT s.*
    FROM metrics_departments s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.hire_date < s.hire_date
        )
),
eligible AS (
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM employees x
            WHERE x.dept_id = s.id
              AND x.is_remote
        )
)
SELECT
    CASE WHEN last3_sum_salary >= 669000 THEN 'PREMIUM' ELSE 'STANDARD' END AS last3_sum_salary_tier,
    DATE_TRUNC('MONTH', hire_date) AS activity_month,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2,
    PERCENTILE_APPROX(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
    COUNT(DISTINCT id) AS distinct_ids_3,
    COUNT(DISTINCT id) AS distinct_ids_4,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN last3_sum_salary >= 669000 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_4_dense_rank
FROM eligible
GROUP BY CASE WHEN last3_sum_salary >= 669000 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', hire_date)
ORDER BY distinct_ids_3;
