SELECT id, product_id, units FROM store_sales WHERE revenue < 2200 UNION ALL SELECT id, product_id, units FROM store_sales WHERE units <= 12;
