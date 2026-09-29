SELECT * FROM departments WHERE (dept_name IS NOT NULL AND budget > 2518000) OR dept_name IN ('HR', 'Legal', 'Marketing')
