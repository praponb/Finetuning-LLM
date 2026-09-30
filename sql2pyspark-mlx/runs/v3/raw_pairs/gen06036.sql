SELECT location, COUNT(*) AS department_count FROM departments WHERE budget IS NOT NULL AND budget >= 2571000 GROUP BY 1 HAVING department_count > 2;
