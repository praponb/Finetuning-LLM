SELECT p.brand, COUNT(ss.id) AS store_sale_count FROM products p LEFT OUTER JOIN store_sales ss ON p.id = ss.product_id GROUP BY p.brand ORDER BY brand DESC, 2 DESC;
