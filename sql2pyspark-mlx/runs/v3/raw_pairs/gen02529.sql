SELECT id, dept_name, budget, location FROM departments WHERE budget IS NOT NULL AND budget <= 2084000 ORDER BY id DESC;
