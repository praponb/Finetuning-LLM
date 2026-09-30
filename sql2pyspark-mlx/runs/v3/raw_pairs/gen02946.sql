SELECT *
FROM (
    SELECT
        id,
        city,
        age,
        country,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC, id) AS row_num
    FROM customers
    WHERE city IS NOT NULL
) AS t
WHERE row_num <= 5;
