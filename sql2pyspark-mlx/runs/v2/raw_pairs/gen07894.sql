SELECT id, product_id, revenue
FROM (
    SELECT
        id,
        product_id,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS rnk
    FROM store_sales
) AS ranked
WHERE rnk <= 3
ORDER BY id DESC
