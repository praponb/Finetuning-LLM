SELECT dept_name, location FROM departments WHERE budget >= 3894000 EXCEPT SELECT dept_name, location FROM departments WHERE budget > 2693000 ORDER BY location DESC;
