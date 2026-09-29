SELECT dept_name, location FROM departments WHERE budget >= 2723000 EXCEPT SELECT dept_name, location FROM departments WHERE location IN ('Chiang Mai', 'Bangkok', 'Berlin');
