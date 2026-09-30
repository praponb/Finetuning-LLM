SELECT * FROM (SELECT dept_name, location, budget FROM departments) src PIVOT (MAX(budget) AS amount, COUNT(1) AS cnt FOR location IN ('Berlin' AS berlin, 'Singapore' AS singapore))
