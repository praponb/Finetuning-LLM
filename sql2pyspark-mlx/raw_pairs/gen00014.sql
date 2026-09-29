SELECT id, dept_name, budget FROM departments WHERE location = 'Bangkok' AND budget BETWEEN 2412000 AND 3920000 AND location IN ('London', 'New York', 'Berlin') ORDER BY id DESC;
