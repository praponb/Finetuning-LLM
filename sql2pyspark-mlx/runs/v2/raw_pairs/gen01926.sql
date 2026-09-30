SELECT
    product_id,
    MAX(sale_date) AS last_sale_date,
    MIN(sale_date) AS first_sale_date,
    COUNT(*) AS num_store_sales
FROM store_sales
WHERE product_id IN (6, 9)
GROUP BY product_id;
