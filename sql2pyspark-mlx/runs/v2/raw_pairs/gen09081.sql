SELECT * FROM (SELECT dept_name, location, budget FROM departments) PIVOT (COUNT(budget) FOR location IN ('Tokyo', 'Chiang Mai', 'Bangkok'))
