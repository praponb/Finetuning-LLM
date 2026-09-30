SELECT job_title, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(job_title))) AS job_title_values
FROM employees
WHERE UPPER(TRIM(email)) = 'FRANK@COMPANY.CO' AND (hire_date > '2024-02-24' OR level IS NULL)
GROUP BY job_title
ORDER BY job_title;
