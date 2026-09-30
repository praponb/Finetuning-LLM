SELECT i.id, i.product_id, p.brand, p.cost, s.rating, s.country FROM inventory i FULL OUTER JOIN products p ON i.product_id = p.id FULL OUTER JOIN suppliers s ON p.supplier_id = s.id
