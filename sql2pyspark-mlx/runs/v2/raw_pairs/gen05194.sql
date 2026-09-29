SELECT
    id,
    store_id,
    revenue,
    product_id,
    FIRST_VALUE(product_id) OVER (PARTITION BY store_id ORDER BY revenue, id) AS first_product_id
FROM store_sales
WHERE sale_date < '2023-04-09'
