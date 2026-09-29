SELECT
    id,
    level,
    full_name,
    CAST(is_remote AS INT) AS is_remote_flag,
    CAST(is_remote AS INT) AS is_remote_flag_2
FROM employees;
