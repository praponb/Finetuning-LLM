SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget DESC, id) AS rn FROM departments) AS t WHERE rn <= 5 ORDER BY location DESC;
