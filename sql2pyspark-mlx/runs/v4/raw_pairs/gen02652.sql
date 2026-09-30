SELECT ss.units, ss.revenue, ss.product_id, p.cost, p.brand, p.supplier_id FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id
