SELECT id, location, budget FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) AS pos FROM departments) AS t WHERE pos <= 3;
