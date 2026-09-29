SELECT i.store_id, ss.revenue FROM inventory i INNER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE ss.revenue BETWEEN 1100 AND 2300 ORDER BY i.store_id DESC;
