SELECT
    id,
    dept_name,
    location,
    budget,
    NTILE(10) OVER (PARTITION BY dept_name, location ORDER BY budget DESC, id) AS budget_bucket,
    SUM(budget) OVER (PARTITION BY location, dept_name ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_budget,
    ROW_NUMBER() OVER (ORDER BY budget, id) AS event_seq,
    LAST_VALUE(budget) IGNORE NULLS OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_budget,
    NTILE(3) OVER (PARTITION BY location ORDER BY budget, id) AS budget_bucket_2
FROM departments;
