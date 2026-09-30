WITH store_sales_by_product AS (
    SELECT product_id, COUNT(units) AS units_count
    FROM store_sales
    GROUP BY product_id
)
SELECT products.id, products.supplier_id, store_sales_by_product.units_count
FROM products
LEFT OUTER JOIN store_sales_by_product ON products.id = store_sales_by_product.product_id
WHERE store_sales_by_product.units_count > 9
