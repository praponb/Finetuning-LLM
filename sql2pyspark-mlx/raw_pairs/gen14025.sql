SELECT ss.id, ss.store_id, ss.sale_date, p.cost FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id ORDER BY ss.id DESC LIMIT 3;
