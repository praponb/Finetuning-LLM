SELECT ss.id, ss.revenue, p.price, s.city FROM store_sales ss JOIN products p ON ss.product_id = p.id LEFT JOIN suppliers s ON p.supplier_id = s.id ORDER BY revenue DESC, id;
