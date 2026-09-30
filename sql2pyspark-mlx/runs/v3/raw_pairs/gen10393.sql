WITH store_sales_by_product AS (
    SELECT product_id, COUNT(DISTINCT revenue) AS unique_stores, COUNT(*) AS store_sale_count
    FROM store_sales
    GROUP BY product_id
)
SELECT p.brand, ssbp.unique_stores, ssbp.store_sale_count
FROM products p
JOIN store_sales_by_product ssbp ON p.id = ssbp.product_id
WHERE ssbp.store_sale_count > 7
ORDER BY p.brand, store_sale_count DESC;
