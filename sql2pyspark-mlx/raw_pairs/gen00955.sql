SELECT ss.store_id, ss.revenue, p.product_name, p.supplier_id, p.price FROM store_sales ss INNER JOIN products p ON p.id = ss.product_id WHERE ss.units <= 38 ORDER BY product_name
