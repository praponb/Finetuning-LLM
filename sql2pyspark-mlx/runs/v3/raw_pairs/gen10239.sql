SELECT product_id, MAX(sale_date) latest_sale_date FROM store_sales GROUP BY product_id ORDER BY product_id;
