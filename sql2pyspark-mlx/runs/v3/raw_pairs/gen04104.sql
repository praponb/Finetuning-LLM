SELECT product_id, store_id, MAX(sale_date) AS last_sale_date FROM store_sales GROUP BY product_id, store_id ORDER BY store_id DESC, product_id ASC LIMIT 1
