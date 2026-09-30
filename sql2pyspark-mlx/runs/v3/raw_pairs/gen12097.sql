WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS row_num
    FROM stores
)
SELECT id, city, opened_date
FROM Ranked
WHERE row_num <= 2;
