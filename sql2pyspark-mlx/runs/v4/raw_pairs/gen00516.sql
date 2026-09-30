SELECT
    id,
    manager_id,
    level,
    hire_date,
    COUNT(*) OVER (PARTITION BY manager_id) manager_id_count,
    FIRST_VALUE(manager_id) OVER (PARTITION BY level ORDER BY hire_date, id) first_manager_id
FROM employees;
