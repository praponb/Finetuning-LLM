SELECT ss.product_id, ss.revenue, p.stock_qty, s.country FROM store_sales ss LEFT OUTER JOIN products p ON ss.product_id = p.id JOIN suppliers s ON p.supplier_id = s.id
