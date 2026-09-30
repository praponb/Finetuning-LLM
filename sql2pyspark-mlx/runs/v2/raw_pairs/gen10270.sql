SELECT products.product_name, products.price, store_sales.product_id FROM products LEFT OUTER JOIN store_sales ON store_sales.product_id = products.id ORDER BY price;
