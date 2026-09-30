WITH valid AS (
    SELECT
        *,
        INITCAP(TRIM(location)) AS location_title,
        COALESCE(location, 'DIRECT') AS resolved_location
    FROM departments
    WHERE location IS NOT NULL
      AND location NOT IN ('Chiang Mai', 'Berlin')
),
scored AS (
    SELECT
        *,
        LAST_VALUE(budget) IGNORE NULLS OVER w_running AS last_known_budget,
        AVG(budget) OVER (ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS budget_avg_2_prior,
        COUNT(budget) OVER (PARTITION BY dept_name) AS dept_name_count_budget
    FROM valid
    WHERE budget < 4018000
    WINDOW w_running AS (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    SELECT
        id,
        dept_name,
        location,
        resolved_location,
        last_known_budget,
        budget_avg_2_prior,
        dept_name_count_budget,
        AVG(dept_name_count_budget) OVER (PARTITION BY dept_name ORDER BY dept_name_count_budget, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS dept_name_count_budget_avg_3,
        LEAD(dept_name_count_budget, 2) OVER (PARTITION BY location ORDER BY budget, id) AS next_dept_name_count_budget,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM scored
    WINDOW w_ordered AS (PARTITION BY location ORDER BY budget_avg_2_prior DESC, id)
),
with_latest AS (
    SELECT
        s.id,
        s.resolved_location,
        s.dept_name_count_budget,
        s.next_dept_name_count_budget,
        s.event_seq,
        top1.level,
        top1.job_title
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.level, c.job_title
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    CASE WHEN next_dept_name_count_budget >= 16 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_dept_name_count_budget_tier,
    COUNT(CASE WHEN job_title = 'Designer' THEN 1 END) AS designer_count,
    AVG(dept_name_count_budget) AS avg_dept_name_count_budget,
    COUNT(*) AS row_count,
    GROUPING_ID() AS grouping_level
FROM with_latest
GROUP BY CUBE (CASE WHEN next_dept_name_count_budget >= 16 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY next_dept_name_count_budget_tier NULLS FIRST;
