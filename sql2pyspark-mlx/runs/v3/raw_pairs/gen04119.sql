SELECT p.id, p.supplier_id, p.cost, p.brand FROM products p WHERE p.category NOT IN ('Sports', 'Books', 'Grocery') AND p.id IN (SELECT product_id FROM store_sales WHERE units BETWEEN 28 AND 39);
