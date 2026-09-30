SELECT
    id,
    manager_id,
    hire_date,
    CASE WHEN level IN ('L2', 'L4') THEN 'Tier 1' ELSE 'Other' END AS level_group,
    IF(is_remote, 'Yes', 'No') AS is_remote_label
FROM employees
