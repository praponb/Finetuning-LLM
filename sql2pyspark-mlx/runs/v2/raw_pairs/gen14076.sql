SELECT ss.sale_date, p.product_name, s.rating FROM store_sales ss RIGHT JOIN products p ON ss.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id WHERE ss.revenue > 3700;
