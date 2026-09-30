WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY job_title ORDER BY hire_date DESC NULLS LAST) AS rank_in_group
    FROM employees
    QUALIFY rank_in_group = 1
)
SELECT
    CASE WHEN salary >= 119000 THEN 'LARGE' ELSE 'SMALL' END AS salary_tier,
    COUNT(*) AS num_rows,
    COUNT(*) FILTER (WHERE level = 'L3') AS l3_rows,
    PERCENTILE_APPROX(salary, 0.5) AS salary_p50,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN job_title = 'Analyst' THEN 1 END), 0), 2) AS salary_per_analyst,
    STDDEV_SAMP(salary) AS salary_stddev
FROM ranked
GROUP BY CASE WHEN salary >= 119000 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(*) > 1
ORDER BY salary_p50 NULLS FIRST
