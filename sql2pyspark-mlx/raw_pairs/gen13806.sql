SELECT id, product_id, revenue
FROM (
    SELECT
        id,
        product_id,
        revenue,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS rn
    FROM store_sales
) AS r
WHERE rn <= 5
