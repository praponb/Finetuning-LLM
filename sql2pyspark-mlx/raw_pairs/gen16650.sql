WITH Numbered AS (
    SELECT
        id,
        city,
        rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS row_num
    FROM suppliers
)
SELECT *
FROM Numbered
WHERE row_num <= 5
ORDER BY city
