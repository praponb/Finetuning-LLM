SELECT
    d.budget,
    e.is_remote,
    e.dept_id,
    e.hire_date,
    CASE WHEN d.location IN ('Chiang Mai', 'London') THEN 'Tier 1' ELSE 'Other' END AS location_group
FROM departments d
LEFT OUTER JOIN employees e ON d.id = e.dept_id;
