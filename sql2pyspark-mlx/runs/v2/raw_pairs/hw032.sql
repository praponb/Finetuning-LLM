SELECT i.store_id, i.product_id, i.quantity, s.units
FROM inventory i
JOIN store_sales s ON i.store_id = s.store_id AND i.product_id = s.product_id;
