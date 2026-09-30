SELECT i.id, i.product_id, i.store_id, ss.units FROM inventory i LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE i.last_restock < '2025-03-06'
