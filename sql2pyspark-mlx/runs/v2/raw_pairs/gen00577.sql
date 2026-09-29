SELECT i.id, i.last_restock, p.launch_date, p.cost, p.brand FROM inventory i INNER JOIN products p ON i.product_id = p.id ORDER BY p.brand, last_restock;
