SELECT i.quantity, s.store_name, p.category
FROM inventory i
INNER JOIN stores s ON i.store_id = s.id
INNER JOIN products p ON i.product_id = p.id
