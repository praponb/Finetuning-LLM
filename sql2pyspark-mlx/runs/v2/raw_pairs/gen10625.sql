SELECT id, dept_name, budget, TRIM(location) AS clean_location FROM departments WHERE budget > 1279000 AND dept_name IS NOT NULL AND budget > 3371000 ORDER BY id DESC, budget
