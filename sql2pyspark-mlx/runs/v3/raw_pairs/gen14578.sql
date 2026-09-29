SELECT dept_name, location, budget, UPPER(dept_name) AS dept_name_upper FROM departments WHERE location IN ('London', 'Chiang Mai', 'Berlin') AND location IS NULL;
