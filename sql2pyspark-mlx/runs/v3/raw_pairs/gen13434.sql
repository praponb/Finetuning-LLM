SELECT p.brand, COUNT(i.quantity) AS quantity_count FROM inventory i LEFT OUTER JOIN products p ON i.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id GROUP BY p.brand
