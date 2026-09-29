SELECT ss.units, p.product_name, p.category, p.stock_qty FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id
