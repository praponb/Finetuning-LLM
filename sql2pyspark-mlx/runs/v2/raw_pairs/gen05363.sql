SELECT id, location, budget
FROM departments
WHERE location NOT IN ('Bangkok', 'Berlin', 'New York') AND budget > 3211000;
