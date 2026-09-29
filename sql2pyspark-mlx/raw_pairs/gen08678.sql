SELECT id, product_id, revenue
FROM (
    SELECT
        id,
        product_id,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS rn
    FROM store_sales
    WHERE product_id = 1
) AS r
WHERE rn <= 1;
