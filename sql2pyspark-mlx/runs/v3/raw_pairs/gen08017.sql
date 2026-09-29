WITH ranked_departments AS (SELECT *, DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS rn FROM departments) SELECT * FROM ranked_departments WHERE rn <= 3;
