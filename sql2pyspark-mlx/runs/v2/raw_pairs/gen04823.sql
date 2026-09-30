WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY location ORDER BY budget) AS row_num FROM departments) SELECT * FROM ranked WHERE row_num <= 5 ORDER BY id DESC;
