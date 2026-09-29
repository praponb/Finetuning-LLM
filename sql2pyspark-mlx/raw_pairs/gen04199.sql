SELECT * FROM (SELECT location, dept_name, budget FROM departments) src PIVOT (MAX(budget) AS total FOR dept_name IN ('Finance' AS finance, 'Sales' AS sales));
