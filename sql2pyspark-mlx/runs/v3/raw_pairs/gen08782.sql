SELECT
    id,
    location,
    budget,
    dept_name
FROM (
    SELECT
        id,
        location,
        budget,
        dept_name,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC, id) row_num
    FROM departments
) ranked
WHERE row_num <= 3;
