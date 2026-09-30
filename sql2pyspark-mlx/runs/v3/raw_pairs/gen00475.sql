SELECT p.id, p.cost, p.price FROM products p WHERE ARRAY_CONTAINS(p.tags, 'new') AND p.id IN (SELECT product_id FROM store_sales WHERE sale_date <= '2025-03-09') ORDER BY p.price DESC, p.id
