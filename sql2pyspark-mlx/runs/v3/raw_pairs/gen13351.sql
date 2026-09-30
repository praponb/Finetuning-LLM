SELECT
    id,
    location,
    budget,
    RANK() OVER (PARTITION BY location ORDER BY budget DESC) AS rnk
FROM departments;
