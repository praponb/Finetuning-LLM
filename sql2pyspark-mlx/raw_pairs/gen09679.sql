WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS rn
    FROM employees
    QUALIFY rn = 1
)
SELECT
    CASE WHEN salary >= 122000 THEN 'HIGH' ELSE 'LOW' END AS salary_tier,
    level,
    COUNT(*) FILTER (WHERE job_title = 'Designer') AS designer_rows,
    STDDEV_SAMP(salary) AS salary_stddev,
    COUNT(DISTINCT manager_id) AS distinct_manager_ids,
    STDDEV_SAMP(salary) AS salary_stddev_2,
    GROUPING(CASE WHEN salary >= 122000 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM deduped
GROUP BY GROUPING SETS ((CASE WHEN salary >= 122000 THEN 'HIGH' ELSE 'LOW' END, level), (CASE WHEN salary >= 122000 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING salary_stddev >= 0
ORDER BY salary_tier DESC;
