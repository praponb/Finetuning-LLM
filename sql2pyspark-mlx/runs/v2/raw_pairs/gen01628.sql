SELECT store_sales.id, store_sales.revenue, store_sales.sale_date FROM store_sales WHERE store_sales.product_id IN (SELECT id FROM products WHERE supplier_id IS NULL)
