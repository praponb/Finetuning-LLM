SELECT p.stock_qty, p.cost, p.price, ss.revenue, ss.units, ss.store_id FROM products p INNER JOIN store_sales ss ON ss.product_id = p.id WHERE ss.sale_date < '2023-05-04' OR ss.store_id = 3
