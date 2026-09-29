SELECT i.store_id, ss.product_id, FLOOR(ss.revenue) revenue_whole FROM inventory i LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
