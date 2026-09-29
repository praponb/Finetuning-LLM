SELECT ss.product_id, p.stock_qty, s.city, s.region FROM store_sales ss RIGHT JOIN products p ON ss.product_id = p.id INNER JOIN stores s ON ss.store_id = s.id WHERE p.cost < 600 AND ss.units >= 18;
