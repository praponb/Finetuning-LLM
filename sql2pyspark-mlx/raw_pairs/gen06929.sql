SELECT
    e.id,
    e.full_name AS employee_full_name,
    m.salary AS manager_salary,
    CONCAT_WS('-', m.job_title, m.full_name) AS job_title_full_name_key
FROM employees AS e
LEFT JOIN employees AS m ON e.manager_id = m.id
WHERE NOT (m.level IN ('L4', 'L1') AND LOWER(m.email) LIKE '%iam%')
