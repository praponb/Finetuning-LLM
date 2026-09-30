SELECT manager_id, hire_date, full_name, LAST_DAY(hire_date) AS hire_date_month_end FROM employees;
