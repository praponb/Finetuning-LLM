WITH Ranked AS (
    SELECT
        id,
        city,
        rating,
        country,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY rating) AS row_num
    FROM suppliers
)
SELECT *
FROM Ranked
WHERE row_num = 1;
