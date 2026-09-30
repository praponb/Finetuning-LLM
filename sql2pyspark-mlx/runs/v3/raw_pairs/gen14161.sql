SELECT i.id, i.quantity, ss.revenue FROM inventory i JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE YEAR(i.last_restock) = 2023;
