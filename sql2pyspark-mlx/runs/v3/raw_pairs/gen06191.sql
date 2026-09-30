SELECT i.product_id, i.quantity, p.cost, p.product_name, p.price FROM inventory i FULL OUTER JOIN products p ON i.product_id = p.id;
