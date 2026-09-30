WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) row_num
    FROM suppliers
)
SELECT
    id,
    city,
    rating,
    country
FROM ranked
WHERE row_num <= 1;
