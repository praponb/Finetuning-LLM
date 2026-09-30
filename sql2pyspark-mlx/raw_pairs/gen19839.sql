SELECT
    id,
    location,
    budget,
    SUM(budget) OVER (PARTITION BY location ORDER BY budget, id) AS running_budget,
    ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) AS row_num
FROM departments
WHERE location NOT IN ('London', 'Bangkok', 'Singapore');
