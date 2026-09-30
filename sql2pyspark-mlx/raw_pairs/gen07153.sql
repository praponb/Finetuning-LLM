SELECT ss.revenue, ss.product_id, ss.units, p.supplier_id, p.stock_qty FROM store_sales ss LEFT OUTER JOIN products p ON ss.product_id = p.id ORDER BY revenue, ss.units
