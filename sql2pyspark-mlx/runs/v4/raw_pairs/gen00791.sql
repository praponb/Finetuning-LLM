SELECT ss.id, ss.units, p.cost, s.rating FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id WHERE ss.units <= 35;
