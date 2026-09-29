SELECT COUNT(*) AS department_count FROM departments WHERE location IN ('Tokyo', 'Chiang Mai') AND budget > 2608000 ORDER BY department_count;
