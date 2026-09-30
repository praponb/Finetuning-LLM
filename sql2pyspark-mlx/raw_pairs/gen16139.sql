WITH with_latest AS (
    -- Latest matching employee per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.hire_date
    FROM departments s
    LEFT JOIN LATERAL (
            SELECT c.hire_date
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.hire_date >= s.hire_date
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_name,
        location,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY hire_date, id) AS event_seq,
        DENSE_RANK() OVER w_ordered AS budget_dense_rank,
        AVG(budget) OVER w_part AS budget_avg_3
    FROM eligible
    WINDOW w_ordered AS (PARTITION BY MONTH(hire_date) ORDER BY budget DESC NULLS FIRST), w_part AS (PARTITION BY dept_name, location ORDER BY hire_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(budget_avg_3) OVER w_running AS last7_min_budget_avg_3,
        AVG(budget_avg_3) OVER w_running AS last7_avg_budget_avg_3,
        MAX(budget_avg_3) OVER w_running AS last7_max_budget_avg_3,
        MIN(budget_avg_3) OVER w_ordered AS dept_name_min_budget_avg_3
    FROM enriched
    WHERE budget_avg_3 > 1923327
    WINDOW w_running AS (PARTITION BY dept_name, location ORDER BY budget_avg_3, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY dept_name)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND c.manager_id = 8
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(last7_max_budget_avg_3 / SUM(last7_max_budget_avg_3) OVER (PARTITION BY location), 4) AS last7_max_budget_avg_3_share,
        AVG(last7_avg_budget_avg_3) OVER (PARTITION BY dept_name ORDER BY last7_min_budget_avg_3, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS last7_avg_budget_avg_3_avg_3_prior
    FROM kept
)
-- Grouped metrics
SELECT
    location,
    COUNT(CASE WHEN dept_name = 'Marketing' THEN 1 END) AS marketing_count,
    SORT_ARRAY(COLLECT_SET(location)) AS location_list,
    COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) AS hr_count
FROM scored
GROUP BY location
HAVING marketing_count > 1
ORDER BY hr_count NULLS FIRST;
