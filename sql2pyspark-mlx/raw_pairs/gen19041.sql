WITH latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY job_title, manager_id ORDER BY salary DESC, id) AS rank_in_group
    FROM employees
    QUALIFY rank_in_group = 1
)
SELECT
    DATE_TRUNC('QUARTER', hire_date) AS quarter_start,
    manager_id,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN level = 'L1' THEN 1 END), 0), 2) AS salary_per_l1,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list_2
FROM latest
GROUP BY quarter_start, manager_id
HAVING ROUND(SUM(salary) / NULLIF(COUNT(CASE WHEN level = 'L1' THEN 1 END), 0), 2) >= 0
