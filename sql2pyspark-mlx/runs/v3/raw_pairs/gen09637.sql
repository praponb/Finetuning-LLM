WITH ranked_products AS (
    SELECT
        id,
        category,
        price,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY price DESC) AS rn
    FROM products
    WHERE launch_date < '2023-05-01'
)
SELECT *
FROM ranked_products
WHERE rn = 1;
