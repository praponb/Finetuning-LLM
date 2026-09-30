SELECT id, product_id, units FROM store_sales WHERE units <= 37 UNION ALL SELECT id, product_id, units FROM store_sales WHERE units > 30
