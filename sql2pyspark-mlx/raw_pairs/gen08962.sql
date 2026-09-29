SELECT ss.id, ss.sale_date, p.price, p.cost, s.opened_date FROM store_sales ss INNER JOIN products p ON p.id = ss.product_id INNER JOIN stores s ON ss.store_id = s.id ORDER BY s.opened_date ASC
