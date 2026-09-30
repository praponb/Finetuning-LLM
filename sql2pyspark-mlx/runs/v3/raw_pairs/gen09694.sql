SELECT manager_id, DATE_FORMAT(hire_date, 'yyyy-MM-dd') AS hire_date_label, SPLIT(LOWER(email), '@')[0] AS email_user FROM employees WHERE level != 'L1';
