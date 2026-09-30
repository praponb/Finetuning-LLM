SELECT
    d.dept_name,
    d.location,
    d.budget,
    e.email,
    e.is_remote,
    e.full_name,
    CASE WHEN d.dept_name IN ('HR', 'Sales') THEN 'Tier 1' ELSE 'Other' END AS dept_name_group
FROM departments d
LEFT OUTER JOIN employees e ON e.dept_id = d.id
WHERE e.hire_date <= '2025-01-02' OR d.budget BETWEEN 1206000 AND 2334000
