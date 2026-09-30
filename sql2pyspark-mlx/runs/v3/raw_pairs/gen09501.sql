SELECT ss.sale_date, p.stock_qty, p.cost FROM store_sales ss RIGHT JOIN products p ON ss.product_id = p.id ORDER BY stock_qty DESC
