SELECT
    id,
    product_id,
    revenue,
    NTILE(2) OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS revenue_ntile
FROM store_sales
WHERE store_id IN (2, 4, 5);
