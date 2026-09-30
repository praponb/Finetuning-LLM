SELECT ss.id, ss.revenue, ss.units, p.stock_qty, p.cost, s.city, s.country FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id LEFT JOIN suppliers s ON p.supplier_id = s.id;
