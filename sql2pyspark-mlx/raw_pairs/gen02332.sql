SELECT product_id, store_id, MAX(revenue) max_revenue, MAX(sale_date) last_sale_date FROM store_sales GROUP BY product_id, store_id ORDER BY product_id DESC;
