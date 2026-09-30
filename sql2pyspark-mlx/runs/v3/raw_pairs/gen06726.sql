SELECT id, location FROM departments WHERE dept_name IN ('HR', 'Legal', 'Sales') AND budget BETWEEN 1437000 AND 2569000 AND dept_name = 'Finance' ORDER BY 2 DESC, id LIMIT 10;
