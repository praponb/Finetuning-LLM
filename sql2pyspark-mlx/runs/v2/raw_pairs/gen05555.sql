SELECT
    id,
    revenue,
    product_id,
    ROW_NUMBER() OVER (ORDER BY revenue DESC, id) AS revenue_row,
    NTILE(3) OVER (PARTITION BY product_id ORDER BY revenue DESC, id) AS revenue_ntile
FROM store_sales
WHERE revenue BETWEEN 3100 AND 3600;
