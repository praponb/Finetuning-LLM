SELECT
    products.supplier_id,
    products.cost,
    products.price,
    store_sales.store_id,
    store_sales.revenue
FROM products
LEFT OUTER JOIN store_sales ON store_sales.product_id = products.id
WHERE store_sales.store_id IN (1, 2, 3) OR store_sales.revenue <= 2800;
