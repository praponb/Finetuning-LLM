SELECT ss.id, ss.sale_date, ss.store_id, s.opened_date, p.cost FROM store_sales ss LEFT OUTER JOIN stores s ON ss.store_id = s.id INNER JOIN products p ON ss.product_id = p.id WHERE p.price < 700
