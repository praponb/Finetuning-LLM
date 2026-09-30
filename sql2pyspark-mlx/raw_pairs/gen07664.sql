SELECT ss.store_id, ss.revenue, p.cost, s.supplier_name, s.country FROM store_sales ss RIGHT OUTER JOIN products p ON ss.product_id = p.id JOIN suppliers s ON p.supplier_id = s.id;
