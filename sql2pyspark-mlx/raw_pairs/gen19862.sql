WITH deduped AS (
    SELECT id, location
    FROM departments
    QUALIFY RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) = 1
),
with_last AS (
    SELECT
        s.id,
        s.location,
        top1.salary
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.salary
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.hire_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.level != 'L5'
        )
),
enriched AS (
    SELECT
        *,
        SUM(salary) OVER w_grp AS running_sum_salary,
        COUNT(salary) OVER w_grp AS running_count_salary,
        MIN(salary) OVER w_grp AS running_min_salary,
        AVG(salary) OVER w_grp AS running_avg_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY location), 4) AS salary_share
    FROM not_flagged
    WINDOW w_grp AS (PARTITION BY location ORDER BY salary, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN running_count_salary >= 14 THEN 'HIGH' ELSE 'LOW' END AS running_count_salary_tier,
    location,
    PERCENTILE_APPROX(running_sum_salary, 0.9) AS running_sum_salary_p90,
    SIZE(COLLECT_SET(location)) AS num_locations,
    COUNT(CASE WHEN location = 'New York' THEN 1 END) AS new_york_count
FROM enriched
GROUP BY ROLLUP (CASE WHEN running_count_salary >= 14 THEN 'HIGH' ELSE 'LOW' END, location)
ORDER BY new_york_count DESC NULLS FIRST
