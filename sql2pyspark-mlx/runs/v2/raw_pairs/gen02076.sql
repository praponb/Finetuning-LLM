SELECT ss.units, ss.sale_date, ss.store_id, p.price FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id;
