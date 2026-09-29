SELECT * FROM (SELECT location, dept_name, budget FROM departments) src PIVOT (MAX(budget) AS total, COUNT(1) AS item_count FOR dept_name IN ('Sales', 'HR'));
