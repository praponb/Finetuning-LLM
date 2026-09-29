SELECT p.launch_date, i.store_id, i.last_restock, MONTH(i.last_restock) AS last_restock_month FROM products p LEFT JOIN inventory i ON p.id = i.product_id;
