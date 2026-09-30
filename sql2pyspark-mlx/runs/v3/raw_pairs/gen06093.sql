WITH ranked AS (
    SELECT
        id,
        brand,
        cost,
        category,
        RANK() OVER (PARTITION BY brand ORDER BY cost) AS rnk
    FROM products
)
SELECT *
FROM ranked
WHERE rnk = 1;
