SELECT id, dept_name, budget FROM departments WHERE location IN ('London', 'Chiang Mai', 'New York') OR dept_name IN ('Engineering', 'Finance') ORDER BY id DESC LIMIT 10
