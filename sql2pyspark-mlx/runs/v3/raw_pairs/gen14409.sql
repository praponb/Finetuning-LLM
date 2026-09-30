SELECT *
FROM (
    SELECT
        id,
        product_id,
        revenue,
        store_id,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS row_num
    FROM store_sales
) AS r
WHERE row_num <= 2
ORDER BY store_id;
