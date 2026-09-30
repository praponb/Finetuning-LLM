SELECT id, manager_id, CONCAT(job_title, ' - ', level) AS job_title_level, IF(is_remote, 'Yes', 'No') AS is_remote_label FROM employees;
