SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget NULLS LAST, id) AS rank_in_group FROM departments) t WHERE rank_in_group = 1;
