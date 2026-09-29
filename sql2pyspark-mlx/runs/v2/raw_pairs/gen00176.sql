SELECT i.id, i.quantity, i.store_id, ss.sale_date, ss.revenue FROM inventory i LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id ORDER BY revenue DESC;
