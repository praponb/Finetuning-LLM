SELECT
    id,
    location,
    budget,
    DENSE_RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS budget_dense_rank,
    NTILE(3) OVER (PARTITION BY location ORDER BY budget, id) AS budget_ntile
FROM departments
WHERE budget <= 1837000
ORDER BY budget_dense_rank DESC
