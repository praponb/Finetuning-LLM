SELECT
    dept_name,
    COUNT(*) AS cnt,
    STDDEV_SAMP(budget) AS budget_stddev
FROM departments
GROUP BY dept_name
HAVING budget_stddev > 0
ORDER BY cnt DESC
