SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY dept_name, location ORDER BY budget DESC) AS rank_in_group FROM departments) x WHERE rank_in_group = 1
