SELECT ss.id, ss.revenue, ss.sale_date, ss.units FROM store_sales ss WHERE ss.product_id IN (2, 3, 4) AND ss.product_id IN (SELECT id FROM products WHERE price <= 500);
