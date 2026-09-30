SELECT id, revenue, product_id, units * revenue AS revenue_per_unit FROM store_sales WHERE revenue < 3500 OR product_id IN (4, 6) ORDER BY product_id DESC, revenue
