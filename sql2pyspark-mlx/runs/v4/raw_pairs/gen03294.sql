SELECT
    id,
    location,
    budget,
    NTILE(3) OVER (PARTITION BY location ORDER BY budget DESC, id) AS quartile
FROM departments
ORDER BY location DESC
