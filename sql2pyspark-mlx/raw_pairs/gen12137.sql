SELECT
    id,
    dept_name,
    location,
    budget,
    LAG(budget) OVER (PARTITION BY location ORDER BY budget, id) prev_budget,
    LEAD(budget) OVER (PARTITION BY location ORDER BY budget, id) next_budget,
    ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget, id) row_seq,
    LEAD(budget, 1) OVER (PARTITION BY location ORDER BY budget, id) next_budget_2
FROM departments
