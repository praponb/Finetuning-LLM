SELECT ss.revenue, ss.store_id, ss.sale_date, p.price, p.product_name FROM store_sales ss JOIN products p ON ss.product_id = p.id WHERE ss.sale_date >= '2023-04-01';
