SELECT full_name, 'employee' AS kind FROM employees WHERE full_name IS NOT NULL UNION SELECT browser, 'session' AS kind FROM sessions WHERE browser IS NOT NULL;
