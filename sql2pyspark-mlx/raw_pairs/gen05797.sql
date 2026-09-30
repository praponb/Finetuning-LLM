SELECT email FROM employees WHERE email IS NOT NULL UNION SELECT carrier FROM shipments WHERE carrier IS NOT NULL;
