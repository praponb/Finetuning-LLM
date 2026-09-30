WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS row_num
    FROM stores
)
SELECT id, city, opened_date
FROM numbered
WHERE row_num <= 2;
