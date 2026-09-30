SELECT ss.revenue, p.supplier_id, p.stock_qty, s.country FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id ORDER BY stock_qty;
