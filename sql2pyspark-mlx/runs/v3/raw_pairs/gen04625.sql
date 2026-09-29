SELECT product_id, COUNT(*) AS store_sale_count FROM store_sales GROUP BY product_id ORDER BY store_sale_count DESC, product_id;
