WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level, job_title ORDER BY salary DESC, id) AS rn
    FROM employees
    QUALIFY rn = 1
)
SELECT
    job_title,
    SIZE(COLLECT_SET(level)) AS num_levels,
    COUNT(DISTINCT job_title) AS distinct_job_titles
FROM ranked
GROUP BY 1
ORDER BY num_levels DESC NULLS FIRST;
