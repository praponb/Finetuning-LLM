SELECT i.id, i.store_id, ss.sale_date FROM inventory i LEFT OUTER JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id WHERE ss.sale_date > '2023-04-01'
