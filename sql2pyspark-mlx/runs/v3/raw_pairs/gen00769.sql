SELECT ss.revenue, ss.sale_date, p.price, s.country FROM store_sales ss JOIN products p ON p.id = ss.product_id JOIN suppliers s ON p.supplier_id = s.id ORDER BY country DESC;
