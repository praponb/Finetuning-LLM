SELECT ss.revenue, ss.product_id, ss.sale_date, p.stock_qty FROM store_sales ss INNER JOIN products p ON p.id = ss.product_id WHERE p.stock_qty = 125 AND ss.units BETWEEN 32 AND 33
