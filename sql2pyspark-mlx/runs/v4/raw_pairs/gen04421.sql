SELECT product_id, MAX(sale_date) AS last_sale_date FROM store_sales WHERE units IS NULL GROUP BY product_id;
