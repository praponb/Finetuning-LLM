SELECT id, product_id, units - revenue AS revenue_per_unit FROM store_sales WHERE store_id IN (1, 2) ORDER BY product_id DESC;
