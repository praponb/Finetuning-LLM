SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS row_num FROM departments) AS ranked WHERE row_num <= 3 ORDER BY id DESC
