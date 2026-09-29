SELECT id, full_name, hire_date, REGEXP_REPLACE(email, ' ', '_') AS email_slug, LAST_DAY(hire_date) AS hire_date_month_end FROM employees WHERE NOT (is_remote AND salary < 131000) ORDER BY hire_date
