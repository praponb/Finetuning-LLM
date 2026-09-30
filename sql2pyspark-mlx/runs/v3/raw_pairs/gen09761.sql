SELECT
    id,
    location,
    budget,
    PERCENT_RANK() OVER (PARTITION BY location ORDER BY budget) budget_pct_rank,
    ROW_NUMBER() OVER (ORDER BY budget DESC, id) budget_row
FROM departments
WHERE budget < 1103000
ORDER BY budget_row DESC
