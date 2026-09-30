SELECT i.id, i.product_id, ss.sale_date, ss.revenue, ss.units * ss.revenue AS revenue_per_unit FROM inventory i LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
