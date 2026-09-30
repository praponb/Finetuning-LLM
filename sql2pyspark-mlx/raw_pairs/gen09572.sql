SELECT products.id, products.supplier_id, products.category
FROM products
WHERE products.id IN (SELECT product_id FROM store_sales WHERE units IS NOT NULL);
