SELECT
    id,
    dept_name,
    budget,
    location,
    DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget DESC) AS dense_rnk,
    LAG(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) AS prev_budget
FROM departments;
