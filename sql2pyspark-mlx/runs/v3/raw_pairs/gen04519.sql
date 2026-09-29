SELECT * FROM employees WHERE NOT (is_remote AND email LIKE '%@yahoo.com') ORDER BY id DESC;
