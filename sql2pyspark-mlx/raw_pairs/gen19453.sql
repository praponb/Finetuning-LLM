WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) rnk FROM departments) SELECT id, dept_name, budget FROM numbered WHERE rnk = 1;
