SELECT products.product_name, store_sales.product_id
FROM products
LEFT JOIN store_sales ON store_sales.product_id = products.id;
