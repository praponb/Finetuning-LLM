WITH ranked AS (SELECT *, DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) rn FROM departments WHERE location <> 'Bangkok') SELECT * FROM ranked WHERE rn <= 1
