SELECT p.id, p.supplier_id FROM products p WHERE p.category IN ('Grocery', 'Home', 'Books') AND p.id IN (SELECT product_id FROM inventory WHERE store_id IN (3, 6))
